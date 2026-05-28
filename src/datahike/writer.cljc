(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?- go-try go-try-]]
            [replikativ.logging :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.gc :as gc]
            [datahike.store :as ds]
            [datahike.config :as dc]
            [datahike.tools :as dt :refer [throwable-promise get-time-ms]]
            [konserve.store :as ks]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <! >! poll! buffer timeout]]
            #?(:cljs [cljs.core.async.impl.channels :refer [ManyToManyChannel]]))
  #?(:clj (:import [clojure.core.async.impl.channels ManyToManyChannel])))

(defn chan? [x]
  (instance? ManyToManyChannel x))

(defprotocol PWriter
  (-dispatch! [_ arg-map] "Returns a channel that resolves when the transaction finalizes.")
  (-shutdown [_] "Returns a channel that resolves when the writer has shut down.")
  (-streaming? [_] "Returns whether the transactor is streaming updates directly into the connection, so it does not need to fetch from store on read."))

(defrecord LocalWriter [thread streaming? transaction-queue-size commit-queue-size
                        transaction-queue commit-queue]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! transaction-queue (assoc arg-map :callback p))
      p))
  (-shutdown [_]
    (close! transaction-queue)
    thread)
  (-streaming? [_] streaming?))

(defrecord RemoteWalWriter [thread streaming? transaction-queue-size transaction-queue]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! transaction-queue (assoc arg-map :callback p))
      p))
  (-shutdown [_]
    (close! transaction-queue)
    thread)
  (-streaming? [_] streaming?))

(def ^:const DEFAULT_QUEUE_SIZE 120000)

;; minimum wait time between commits in ms
;; this reduces write pressure on the storage
;; at the cost of higher latency
(def ^:const DEFAULT_COMMIT_WAIT_TIME 0) ;; in ms

(defn create-thread
  "Creates new transaction thread"
  [connection write-fn-map transaction-queue-size commit-queue-size commit-wait-time]
  (let [transaction-queue-buffer    (buffer transaction-queue-size)
        transaction-queue           (chan transaction-queue-buffer)
        commit-queue-buffer         (buffer commit-queue-size)
        commit-queue                (chan commit-queue-buffer)]
    [transaction-queue commit-queue
     (#?(:clj thread-try :cljs try)
      S
      (do
        ;; processing loop
        (go-try S
         ;; delay processing until the writer we are part of in connection is set
                (while (not (:writer @(:wrapped-atom connection)))
                  (<! (timeout 10)))
                (loop [old @(:wrapped-atom connection)]
                  (if-let [{:keys [op args callback] :as invocation} (<?- transaction-queue)]
                    (do
                      (when (> (count transaction-queue-buffer) (* 0.9 transaction-queue-size))
                        (log/warn :datahike/tx-queue-pressure "Transaction queue buffer >90% full" {:count (count transaction-queue-buffer) :size transaction-queue-size}))
                      (let [;; TODO remove this after import is ported to writer API
                            old (if-not (= (:max-tx old) (:max-tx @(:wrapped-atom connection)))
                                  (assoc old :max-tx (:max-tx @(:wrapped-atom connection)))
                                  old)

                            op-fn (write-fn-map op)
                            res   (try
                                    (apply op-fn old args)
                            ;; Catch all Throwables to handle AssertionError and other Errors
                            ;; These should crash the writer, but we deliver to callback first to prevent hangs
                                    (catch #?(:clj Throwable :cljs js/Error) e
                                      (log/error :datahike/write-error {:invocation invocation :error e :args args})
                              ;; take a guess that a NPE was triggered by an invalid connection
                              ;; short circuit on errors
                                      #?(:cljs (put! callback e)
                                         :clj
                                         (put! callback
                                               (if (= (type e) NullPointerException)
                                                 (ex-info "Null pointer encountered in invocation. Connection may have been invalidated, e.g. through db deletion, and needs to be released everywhere."
                                                          {:type       :writer-error-during-invocation
                                                           :invocation invocation
                                                           :connection connection
                                                           :error      e})
                                                 e)))
                              ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                              ;; Only Exceptions should be handled and allow the writer to continue
                                      #?(:clj (when (instance? Error e)
                                                (throw e)))
                                      :error))]
                        (cond (chan? res)
                              ;; async op, run in parallel in background, no sequential commit handling needed
                              (do
                                (go (>! callback (<! res)))
                                (recur old))

                              (not= res :error)
                              (do
                                (when (> (count commit-queue-buffer) (/ commit-queue-size 2))
                                  (log/warn :datahike/commit-queue-pressure "Commit queue buffer >50% full" {:count (count commit-queue-buffer) :size commit-queue-size})
                                  (<! (timeout 50)))
                                (put! commit-queue [res callback])
                                (recur (:db-after res)))
                              :else
                              (recur old))))
                    (do
                      (close! commit-queue)
                      (log/debug :datahike/writer-closed "Writer thread gracefully closed")))))
        ;; commit loop
        (go-try S
                (loop [tx (<?- commit-queue)]
                  (when tx
                    (let [txs (into [tx] (take-while some?) (repeatedly #(poll! commit-queue)))]
              ;; empty channel of pending transactions
                      (log/trace :datahike/batch-commit {:batch-size (count txs)})
              ;; commit latest tx to disk
                      (let [db (:db-after (first (peek txs)))
                            ;; Check for merge parents (set by merge-writer!)
                            merge-parents (get-in db [:meta :datahike/merge-parents])
                            ;; Clear merge-parents from db meta before persisting
                            db (if merge-parents
                                 (update db :meta dissoc :datahike/merge-parents)
                                 db)]
                        (try
                          (let [start-ts (get-time-ms)
                                {{:keys [datahike/commit-id]} :meta
                                 :as commit-db} (<?- (w/commit! db merge-parents false))
                                commit-time (- (get-time-ms) start-ts)]
                            (log/trace :datahike/commit-time {:duration-ms commit-time})
                            (reset! connection commit-db)
                    ;; notify all processes that transaction is complete
                            (doseq [[tx-report callback] txs]
                              (let [tx-report (-> tx-report
                                                  (assoc-in [:tx-meta :db/commitId] commit-id)
                                                  (assoc :db-after commit-db))]
                                (>! callback tx-report))))
                          (catch #?(:clj Throwable :cljs js/Error) e
                            (doseq [[_ callback] txs]
                              (put! callback e))
                            (log/error :datahike/writer-shutdown {:error e})
                            (close! commit-queue)
                            (close! transaction-queue)
                            ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                            #?(:clj (when (instance? Error e)
                                      (throw e)))))
                        (<! (timeout commit-wait-time))
                        (recur (<?- commit-queue)))))))))]))

(defn- remote-wal-op-fn [op]
  (case op
    transact! w/transact!
    load-entities w/load-entities
    nil))

(defn- remote-wal-attach-runtime [db runtime-db]
  (cond-> db
    (:store runtime-db)
    (assoc :store (:store runtime-db))
    (:writer runtime-db)
    (assoc :writer (:writer runtime-db))
    (:remote-wal-store runtime-db)
    (assoc :remote-wal-store (:remote-wal-store runtime-db)
           :remote-wal-store-config (:remote-wal-store-config runtime-db))))

(defn- remote-wal-writer-id [{:keys [config]}]
  (or (get-in config [:writer :writer-id])
      (get-in config [:store :id])
      "datahike-remote-wal-writer"))

(defn- validate-remote-wal-head! [wal-read wal-key]
  (when-not (:exists? wal-read)
    (log/raise "Remote WAL head does not exist. Use create-database before connect/transact."
               {:type :remote-wal/head-missing
                :wal-key wal-key}))
  (when-not (w/remote-wal-record? (:value wal-read))
    (log/raise "Remote WAL head object is not a Datahike remote WAL record."
               {:type :remote-wal/invalid-head
                :wal-key wal-key
                :value (:value wal-read)})))

(defn- validate-remote-wal-tx-report! [tx-report]
  (when (seq (:secondary-indices (:db-after tx-report)))
    (log/raise "Remote WAL writer does not support secondary indexes yet."
               {:type :remote-wal/unsupported-secondary-index})))

(defn- remote-wal-materialize-later! [commit-db wal-id writer-config]
  (when (:wal-auto-materialize? writer-config)
    (go-try S
            (<?- (w/materialize-db-with-cid! commit-db wal-id {:sync? false}))
            (when-let [remote-store (:remote-wal-store commit-db)]
              (<?- (w/remote-materialize-wal! remote-store
                                              (:wal-branch writer-config)
                                              (:config commit-db)
                                              {:sync? false}))))))

(defn- remote-wal-apply-once!
  [connection invocation]
  (go-try-
   (let [{:keys [op args]} invocation
         current-db @(:wrapped-atom connection)
         writer-config (get-in current-db [:config :writer])
         wal-key (:wal-branch writer-config)
         remote-store (:remote-wal-store current-db)
         max-retries (:wal-max-retries writer-config)
         op-fn (remote-wal-op-fn op)]
     (when-not op-fn
       (log/raise "Remote WAL writer does not support this operation yet."
                  {:type :remote-wal/unsupported-operation
                   :op op}))
     (when-not remote-store
       (log/raise "Remote WAL writer is missing its connected remote store."
                  {:type :remote-wal/missing-runtime-store}))
     (loop [attempt 0]
       (let [runtime-db @(:wrapped-atom connection)
             wal-read (<?- (w/get-wal-head-with-etag remote-store wal-key {:sync? false}))
             _ (validate-remote-wal-head! wal-read wal-key)
             wal-record (:value wal-read)
             base-db (remote-wal-attach-runtime
                      (w/sync-db-to-wal-record runtime-db wal-record)
                      runtime-db)
             tx-report (apply op-fn base-db args)
             _ (validate-remote-wal-tx-report! tx-report)
             wal-entry (w/build-wal-entry (:datahike/wal-head wal-record)
                                          (remote-wal-writer-id base-db)
                                          tx-report)
             wal-id (:datahike/wal-id wal-entry)
             new-wal-record (w/append-wal-entry wal-record wal-entry)
             cas-result (<?- (w/cas-assoc! remote-store wal-key (:etag wal-read)
                                           new-wal-record {:sync? false}))]
         (case cas-result
           :ok (let [commit-db (-> (:db-after tx-report)
                                   (assoc-in [:meta :datahike/commit-id] wal-id)
                                   (remote-wal-attach-runtime base-db))
                     tx-report (-> tx-report
                                   (assoc :db-after commit-db)
                                   (assoc-in [:tx-meta :db/commitId] wal-id))]
                 (reset! connection commit-db)
                 (remote-wal-materialize-later! commit-db wal-id writer-config)
                 tx-report)
           :conflict
           ;; Speculative remote-WAL attempts do not flush indexes, so there are
           ;; no attempt-local pending KVs to discard here. Clearing the
           ;; store-level pending-writes atom can race with background
           ;; materialization from an already-won WAL entry.
           (if (< attempt max-retries)
                       (recur (inc attempt))
                       (log/raise "Remote WAL CAS failed after maximum retries."
                                  {:type :remote-wal/cas-retries-exhausted
                                   :attempts (inc attempt)
                                   :wal-key wal-key}))
           (log/raise "Remote WAL CAS helper returned an invalid result."
                      {:type :remote-wal/invalid-cas-result
                       :result cas-result
                       :wal-key wal-key})))))))

(defn create-remote-wal-thread
  "Creates a single-attempt-at-a-time remote WAL writer loop."
  [connection transaction-queue-size]
  (let [transaction-queue-buffer (buffer transaction-queue-size)
        transaction-queue (chan transaction-queue-buffer)]
    [transaction-queue
     (#?(:clj thread-try :cljs try)
      S
      (go-try S
              (while (not (:writer @(:wrapped-atom connection)))
                (<! (timeout 10)))
              (loop []
                (if-let [{:keys [callback] :as invocation} (<?- transaction-queue)]
                  (do
                    (when (> (count transaction-queue-buffer) (* 0.9 transaction-queue-size))
                      (log/warn :datahike/tx-queue-pressure "Remote WAL transaction queue buffer >90% full"
                                {:count (count transaction-queue-buffer)
                                 :size transaction-queue-size}))
                    (try
                      (let [tx-report (<?- (remote-wal-apply-once! connection invocation))]
                        (>! callback tx-report))
                      (catch #?(:clj Throwable :cljs js/Error) e
                        (put! callback e)
                        (log/error :datahike/remote-wal-write-error {:invocation invocation :error e})
                        #?(:clj (when (instance? Error e)
                                  (throw e)))))
                    (recur))
                  (log/debug :datahike/remote-wal-writer-closed "Remote WAL writer thread gracefully closed")))))]))

;; public API to internal mapping
(def default-write-fn-map {'transact!     w/transact!
                           'load-entities w/load-entities
                           ;; async operations that run in background
                           'gc-storage!   gc/gc-storage!
                           ;; secondary index backfill (async, runs in background)
                           #?@(:clj ['build-secondary-index! w/build-secondary-index!
                                     'install-secondary-index! w/install-secondary-index!])
                           ;; merge with multi-parent commit tracking
                           'merge! w/merge-writer!})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))

(defmethod create-writer :self
  [{:keys [transaction-queue-size commit-queue-size write-fn-map commit-wait-time]} connection]
  (let [transaction-queue-size (or transaction-queue-size DEFAULT_QUEUE_SIZE)
        commit-queue-size (or commit-queue-size DEFAULT_QUEUE_SIZE)
        commit-wait-time (or commit-wait-time DEFAULT_COMMIT_WAIT_TIME)
        [transaction-queue commit-queue thread]
        (create-thread connection
                       (merge default-write-fn-map
                              write-fn-map)
                       transaction-queue-size
                       commit-queue-size
                       commit-wait-time)]
    (map->LocalWriter
     {:transaction-queue transaction-queue
      :transaction-queue-size transaction-queue-size
      :commit-queue commit-queue
      :commit-queue-size commit-queue-size
      :thread thread
      :streaming? true})))

(defmethod create-writer :remote-wal
  [{:keys [transaction-queue-size]} connection]
  (let [transaction-queue-size (or transaction-queue-size DEFAULT_QUEUE_SIZE)
        [transaction-queue thread] (create-remote-wal-thread connection transaction-queue-size)]
    (map->RemoteWalWriter
     {:transaction-queue transaction-queue
      :transaction-queue-size transaction-queue-size
      :thread thread
      :streaming? true})))

;; Note: :kabel backend is implemented in datahike.kabel.writer
;; Require that namespace to register the defmethod

(defn dispatch! [writer arg-map]
  (-dispatch! writer arg-map))

(defn shutdown [writer]
  (-shutdown writer))

(defn streaming? [writer]
  (-streaming? writer))

(defn backend-dispatch [& args]
  (get-in (first args) [:writer :backend] :self))

(defmulti create-database backend-dispatch)

(defmethod create-database :self [& args]
  (let [p (throwable-promise)]
    (go
      (#?(:clj deliver :cljs put!) p (<! (apply w/create-database args))))
    p))

(defn- connect-or-create-remote-wal-store [config opts]
  (go-try-
   (let [remote-store-config (get-in config [:writer :remote-store])
         remote-exists? (<?- (ks/store-exists? remote-store-config opts))
         raw-store (<?- ((if remote-exists? ks/connect-store ks/create-store)
                         remote-store-config opts))
         store (ds/add-cache-and-handlers raw-store (assoc config :store remote-store-config))]
     (<?- (ds/ready-store (assoc remote-store-config :opts opts) store))
     store)))

(defmethod create-database :remote-wal [& args]
  (let [p (throwable-promise)]
    (go
      (let [opts {:sync? false}
            local-config (atom nil)
            remote-store (atom nil)
            remote-wal-created? (atom false)]
        (try
          (let [config (<?- (apply w/create-database args))
                _ (reset! local-config config)
                store (<?- (connect-or-create-remote-wal-store config opts))
                _ (reset! remote-store store)
                wal-key (get-in config [:writer :wal-branch])
                initial-wal (w/initial-remote-wal-record wal-key)
                cas-result (<?- (w/cas-assoc! store wal-key nil initial-wal opts))]
            (when-not (= :ok cas-result)
              (log/raise "Remote WAL head already exists."
                         {:type :remote-wal/database-already-exists
                          :wal-key wal-key
                          :cas-result cas-result}))
            (reset! remote-wal-created? true)
            (<?- (ks/release-store (get-in config [:writer :remote-store]) store opts))
            (#?(:clj deliver :cljs put!) p config))
          (catch #?(:clj Throwable :cljs js/Error) e
            (when-let [config @local-config]
              (when-let [store @remote-store]
                (try
                  (<?- (ks/release-store (get-in config [:writer :remote-store]) store opts))
                  (catch #?(:clj Throwable :cljs js/Error) release-error
                    (log/warn :datahike/remote-wal-release-failed {:error release-error}))))
              (when-not @remote-wal-created?
                (try
                  (<?- (w/delete-database config))
                  (catch #?(:clj Throwable :cljs js/Error) cleanup-error
                    (log/warn :datahike/remote-wal-local-create-cleanup-failed
                              {:error cleanup-error
                               :config config})))))
            (#?(:clj deliver :cljs put!) p e)))))
    p))

(defmulti delete-database backend-dispatch)

(defmethod delete-database :self [& args]
  (let [p (throwable-promise)]
    (go
      (let [res (<! (apply w/delete-database args))]
        #?(:clj (deliver p res) :cljs (if (nil? res) (close! p) (put! p res)))))
    p))

(defmethod delete-database :remote-wal [& args]
  (let [p (throwable-promise)]
    (go
      (try
        (let [config (dc/load-config (first args) (next args))
              remote-store-config (get-in config [:writer :remote-store])
              local-res (<! (apply w/delete-database args))
              remote-res (ks/delete-store remote-store-config {:sync? true})]
          #?(:clj (deliver p (or remote-res local-res))
             :cljs (if (nil? (or remote-res local-res))
                     (close! p)
                     (put! p (or remote-res local-res)))))
        (catch #?(:clj Throwable :cljs js/Error) e
          (#?(:clj deliver :cljs put!) p e))))
    p))

(defn- detect-new-building-indices
  "Detect secondary indices that *transitioned* into :building in this tx,
   i.e. they are :building in db-after but were not already :building in
   db-before. Returns a seq of idx-idents that need a one-time backfill.

   Comparing against db-before is essential: any transaction applied while
   an index is still building would otherwise re-dispatch a full backfill,
   and a second backfill that runs after the first one's
   install-secondary-index! has dissoc'd :db.secondary/building-since-tx
   loses the snapshot guard and re-delivers post-creation datoms that were
   already applied live — double-counting them in the index."
  [tx-report]
  (let [before (get-in tx-report [:db-before :schema])
        after  (get-in tx-report [:db-after :schema])]
    (when after
      (keep (fn [[ident entry]]
              (when (and (map? entry)
                         (:db.secondary/type entry)
                         (= :building (:db.secondary/status entry))
                         (not= :building (get-in before [ident :db.secondary/status])))
                ident))
            after))))

(defn transact!
  [connection arg-map]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'transact!
                                      :args [arg-map]}))]
        (when (map? tx-report) ;; not error
          ;; Dispatch backfill for any newly created secondary indices
          #?(:clj
             (doseq [idx-ident (detect-new-building-indices tx-report)]
               (log/trace :datahike/dispatch-backfill {:idx-ident idx-ident})
               ;; build-secondary-index! is async (returns channel).
               ;; When it completes, dispatch install to swap in the result.
               (go
                 (let [build-result (<! (dispatch! writer {:op 'build-secondary-index!
                                                           :args [idx-ident]}))]
                   (when (map? build-result)
                     (dispatch! writer {:op 'install-secondary-index!
                                        :args [build-result]}))))))
          (doseq [[_ callback] (some-> (:listeners (meta connection)) (deref))]
            (callback tx-report)))
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

(defn load-entities [connection entities]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'load-entities
                                      :args [entities]}))]
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

(defn merge-db!
  "Merge parent branches/commits into the current branch through the writer.
   Parents is a set of branch keywords or commit UUIDs.
   tx-data contains the merged changes."
  [connection {:keys [parents tx-data tx-meta] :as arg-map}]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'merge!
                                      :args [arg-map]}))]
        (when (map? tx-report)
          (doseq [[_ callback] (some-> (:listeners (meta connection)) (deref))]
            (callback tx-report)))
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

(defn gc-storage! [conn & args]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom conn))]
    (go
      (let [result (<! (dispatch! writer
                                  {:op 'gc-storage!
                                   :args (vec args)}))]
        (#?(:clj deliver :cljs put!) p result)))
    p))