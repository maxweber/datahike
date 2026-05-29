(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [delete-connection! *connections*]]
            [datahike.db :as db]
            [datahike.db.transaction :as dbt]
            [datahike.db.utils :as dbu]
            [datahike.db.interface :as dbi]
            [datahike.db.search :as dbs]
            [datahike.datom :as dd]
            [datahike.index :as di]
            [datahike.index.audit :as audit]
            [datahike.index.secondary :as sec]
            [datahike.store :as ds]
            [datahike.tools :as dt]
            [datahike.core :as core]
            [datahike.query :as dq]
            [datahike.config :as dc]
            [datahike.schema-cache :as sc]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.store :as ks]
            #?(:clj [konserve.impl.defaults :as konserve-defaults])
            #?(:clj [konserve.impl.storage-layout :as storage-layout])
            #?(:clj [konserve.protocols :refer [-serialize -deserialize]])
            #?(:clj [konserve.memory])
            #?(:clj [clojure.core.cache :as cache])
            [replikativ.logging :as log]
            [hasch.core :refer [uuid squuid]]
            [hasch.platform]
            [clojure.core.async :as async :refer [go put!]]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? meta-update *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])])
  #?(:clj (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
                   [java.util Arrays])))

;; mapping to storage

(defn stored-db? [obj]
  ;; TODO use proper schema to match?
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

;; ---------------------------------------------------------------------------
;; Remote WAL helpers

(declare stored->db)

(def ^:const remote-wal-version 1)

(defprotocol PRemoteWalCasStore
  "Minimal conditional-write interface required by the remote WAL head."
  (-get-with-etag [store key opts]
    "Return {:value v :etag version :exists? boolean} for key.")
  (-cas-assoc! [store key expected-etag new-value opts]
    "Conditionally associate key to new-value. Returns :ok or :conflict."))

(defn datom->wal [d]
  [(:e d) (:a d) (:v d) (dd/datom-tx d) (dd/datom-added d)])

(defn wal->datom [[e a v tx added?]]
  (dd/datom e a v tx added?))

(defn wal-entry-id [entry-without-id]
  (uuid entry-without-id))

(defn- wal-summary-meta [meta]
  ;; The WAL id becomes the winning commit id only after CAS, so do not
  ;; bake a stale parent commit id into the deterministic WAL-entry content.
  ;; Fresh empty DB reconstruction also creates a new local DB id/created-at;
  ;; library versions can legitimately change between WAL append and replay.
  ;; These are not logical WAL state and should not affect replay validation.
  (dissoc meta
          :datahike/commit-id
          :datahike/id
          :datahike/created-at
          :datahike/version
          :hitchhiker.tree/version
          :persistent.set/version
          :konserve/version))

(defn db-summary [{:keys [max-tx max-eid hash meta]}]
  {:max-tx max-tx
   :max-eid max-eid
   :hash hash
   :meta (wal-summary-meta meta)})

(defn build-wal-entry
  ([wal-parent writer-id tx-report]
   (build-wal-entry wal-parent writer-id [tx-report] (dt/get-date)))
  ([wal-parent writer-id tx-reports created-at]
   (let [tx-reports (vec tx-reports)
         db-after (:db-after (peek tx-reports))
         entry-without-id
         {:datahike/wal-entry? true
          :datahike/wal-parent wal-parent
          :datahike/writer-id writer-id
          :datahike/created-at created-at
          :datahike/txs
          (mapv (fn [{:keys [tx-data tx-meta db-after]}]
                  {:tx-data (mapv datom->wal tx-data)
                   :tx-meta (or tx-meta {})
                   :max-tx (:max-tx db-after)
                   :max-eid (:max-eid db-after)})
                tx-reports)
          :datahike/db-after (db-summary db-after)}
         wal-id (wal-entry-id entry-without-id)]
     (assoc entry-without-id :datahike/wal-id wal-id))))

(defn remote-wal-record?
  ([obj]
   (and (map? obj)
        (true? (:datahike/remote-wal? obj))
        (= remote-wal-version (:datahike/wal-version obj))))
  ([obj branch]
   (and (remote-wal-record? obj)
        (= branch (:datahike/branch obj)))))

(defn initial-remote-wal-record [branch]
  {:datahike/remote-wal? true
   :datahike/wal-version remote-wal-version
   :datahike/branch branch
   :datahike/wal-head nil
   :datahike/materialized-head nil
   :datahike/materialized-db nil
   :datahike/pending []})

(defn append-wal-entry [wal-record wal-entry]
  (when-not (remote-wal-record? wal-record)
    (log/raise "Remote WAL head object is missing or has an unsupported format."
               {:type :remote-wal/invalid-head
                :wal-record wal-record}))
  (when-not (= (:datahike/wal-head wal-record)
               (:datahike/wal-parent wal-entry))
    (log/raise "Remote WAL entry parent does not match current WAL head."
               {:type :remote-wal/parent-mismatch
                :wal-head (:datahike/wal-head wal-record)
                :wal-parent (:datahike/wal-parent wal-entry)}))
  (-> wal-record
      (assoc :datahike/wal-head (:datahike/wal-id wal-entry))
      (update :datahike/pending (fnil conj []) wal-entry)))

(defn get-wal-head-with-etag
  ([store key]
   (get-wal-head-with-etag store key {:sync? true}))
  ([store key opts]
   (if (satisfies? PRemoteWalCasStore store)
     (-get-with-etag store key opts)
     (log/raise "Remote WAL store does not expose conditional head read/CAS operations."
                {:type :remote-wal/cas-unavailable
                 :key key
                 :store-type (type store)}))))

(defn cas-assoc!
  ([store key expected-etag new-value]
   (cas-assoc! store key expected-etag new-value {:sync? true}))
  ([store key expected-etag new-value opts]
   (if (satisfies? PRemoteWalCasStore store)
     (-cas-assoc! store key expected-etag new-value opts)
     (log/raise "Remote WAL store does not expose conditional head write/CAS operations."
                {:type :remote-wal/cas-unavailable
                 :key key
                 :store-type (type store)}))))

#?(:clj
   (do
     (defn- update-store-cache! [store key value exists?]
       (when-let [cache-atom (:cache store)]
         (swap! cache-atom cache/evict key)
         (when exists?
           (swap! cache-atom cache/miss key value))))

     (defn- default-store-codec [store serializer compressor encryptor]
       ((encryptor (get-in store [:config :encryptor]))
        (compressor serializer)))

     (defn- default-store-serialize [store serializer value]
       (let [baos (ByteArrayOutputStream.)]
         (try
           (-serialize (default-store-codec store serializer
                                            (:compressor store)
                                            (:encryptor store))
                       baos
                       (:write-handlers store)
                       value)
           (.toByteArray baos)
           (finally
             (.close baos)))))

     (defn- default-store-deserialize [store serializer compressor encryptor bytes]
       (let [bais (ByteArrayInputStream. bytes)]
         (try
           (-deserialize (default-store-codec store serializer compressor encryptor)
                         (:read-handlers store)
                         bais)
           (finally
             (.close bais)))))

     (defn- default-store-blob-bytes [store key old-meta value]
       (let [serializer (get (:serializers store) (:default-serializer store))
             meta (meta-update key :edn old-meta)
             meta-arr (default-store-serialize store serializer meta)
             value-arr (default-store-serialize store serializer value)
             header (storage-layout/create-header (:version store)
                                                  serializer
                                                  (:compressor store)
                                                  (:encryptor store)
                                                  (count meta-arr))
             baos (ByteArrayOutputStream. (+ (alength ^bytes header)
                                             (alength ^bytes meta-arr)
                                             (alength ^bytes value-arr)))]
         (try
           (.write baos ^bytes header)
           (.write baos ^bytes meta-arr)
           (.write baos ^bytes value-arr)
           (.toByteArray baos)
           (finally
             (.close baos)))))

     (defn- default-store-parse-blob [store ^bytes bytes]
       (let [header (Arrays/copyOfRange bytes 0 storage-layout/header-size)
             [_version serializer compressor encryptor meta-size actual-header-size]
             (storage-layout/parse-header header (:serializers store))
             meta-end (+ actual-header-size meta-size)
             meta-arr (Arrays/copyOfRange bytes actual-header-size meta-end)
             value-arr (Arrays/copyOfRange bytes meta-end (alength bytes))]
         {:meta (default-store-deserialize store serializer compressor encryptor meta-arr)
          :value (default-store-deserialize store serializer compressor encryptor value-arr)}))

     (defn- s3-backing? [backing]
       (and backing
            (= "konserve_s3.core.S3Bucket" (.getName (class backing)))))

     (defn- s3-raw-key [store key]
       (str (get-in store [:backing :store-id]) "_" (konserve-defaults/key->store-key key)))

     (defn- resolve-s3-var! [sym]
       (try
         (or (requiring-resolve sym)
             (log/raise "konserve-s3 helper var is unavailable."
                        {:type :remote-wal/s3-cas-unavailable
                         :var sym}))
         (catch Throwable e
           (log/raise "Remote WAL S3/Tigris CAS requires konserve-s3 on the classpath."
                      {:type :remote-wal/s3-cas-unavailable
                       :var sym
                       :error e}))))

     (defn- s3-get-object-with-etag* [backing raw-key]
       ((resolve-s3-var! 'konserve-s3.core/get-object-with-etag)
        (:client backing)
        (:bucket backing)
        raw-key))

     (defn- s3-precondition-failed? [^Throwable e]
       (let [s3-ex-class (try
                           (Class/forName "software.amazon.awssdk.services.s3.model.S3Exception")
                           (catch Throwable _ nil))]
         (boolean
          (and s3-ex-class
               (some (fn [^Throwable t]
                       (and (instance? s3-ex-class t)
                            (= 412 (clojure.lang.Reflector/invokeInstanceMethod
                                    t "statusCode" (object-array [])))))
                     (take-while some? (iterate #(.getCause ^Throwable %) e)))))))

     (defn- s3-put-object-if-none-match [client bucket raw-key ^bytes bytes]
       (let [put-request-class (Class/forName "software.amazon.awssdk.services.s3.model.PutObjectRequest")
             request-body-class (Class/forName "software.amazon.awssdk.core.sync.RequestBody")
             builder (clojure.lang.Reflector/invokeStaticMethod put-request-class "builder" (object-array []))
             builder (clojure.lang.Reflector/invokeInstanceMethod builder "bucket" (object-array [bucket]))
             builder (clojure.lang.Reflector/invokeInstanceMethod builder "key" (object-array [raw-key]))
             builder (clojure.lang.Reflector/invokeInstanceMethod builder "ifNoneMatch" (object-array ["*"]))
             request (clojure.lang.Reflector/invokeInstanceMethod builder "build" (object-array []))
             body (clojure.lang.Reflector/invokeStaticMethod request-body-class "fromBytes" (object-array [bytes]))]
         (try
           (clojure.lang.Reflector/invokeInstanceMethod client "putObject" (object-array [request body]))
           true
           (catch Throwable e
             (if (s3-precondition-failed? e)
               false
               (throw e))))))

     (defn- s3-put-object-conditional* [backing raw-key ^bytes bytes expected-etag]
       (if expected-etag
         ((resolve-s3-var! 'konserve-s3.core/put-object-conditional)
          (:client backing)
          (:bucket backing)
          raw-key
          bytes
          expected-etag)
         (s3-put-object-if-none-match (:client backing) (:bucket backing) raw-key bytes)))

     (defn- s3-etag-token [etag meta]
       {:backend :s3
        :etag etag
        :meta meta})

     (defn- s3-default-store-required! [store key]
       (when-not (s3-backing? (:backing store))
         (log/raise "Remote WAL CAS is only implemented for memory and S3/Tigris konserve stores."
                    {:type :remote-wal/cas-unavailable
                     :key key
                     :store-type (type store)
                     :backing-type (type (:backing store))})))

     (extend-type konserve.impl.defaults.DefaultStore
       PRemoteWalCasStore
       (-get-with-etag [store key opts]
         (s3-default-store-required! store key)
         (async+sync (:sync? opts) *default-sync-translation*
                     (go-try-
                      (let [raw-key (s3-raw-key store key)
                            object (s3-get-object-with-etag* (:backing store) raw-key)]
                        (if object
                          (let [{:keys [meta value]} (default-store-parse-blob store (:data object))]
                            (update-store-cache! store key value true)
                            {:value value
                             :etag (s3-etag-token (:etag object) meta)
                             :exists? true})
                          (do
                            (update-store-cache! store key nil false)
                            {:value nil
                             :etag nil
                             :exists? false}))))))
       (-cas-assoc! [store key expected-etag new-value opts]
         (s3-default-store-required! store key)
         (async+sync (:sync? opts) *default-sync-translation*
                     (go-try-
                      (let [etag (if (map? expected-etag)
                                   (:etag expected-etag)
                                   expected-etag)
                            old-meta (when (map? expected-etag)
                                       (:meta expected-etag))
                            raw-key (s3-raw-key store key)
                            bytes (default-store-blob-bytes store key old-meta new-value)
                            won? (s3-put-object-conditional* (:backing store) raw-key bytes etag)]
                        (if won?
                          (do
                            (update-store-cache! store key new-value true)
                            :ok)
                          (do
                            (update-store-cache! store key nil false)
                            :conflict)))))))

     (extend-type konserve.memory.MemoryStore
       PRemoteWalCasStore
       (-get-with-etag [store key opts]
         (async+sync (:sync? opts) *default-sync-translation*
                     (go-try-
                      (let [state @(:state store)
                            [meta value] (get state key)]
                        {:value value
                         :etag meta
                         :exists? (contains? state key)}))))
       (-cas-assoc! [store key expected-etag new-value opts]
         (async+sync (:sync? opts) *default-sync-translation*
                     (go-try-
                      (let [[old-state _new-state]
                            (swap-vals! (:state store)
                                        (fn [state]
                                          (let [[old-meta _old-value] (get state key)
                                                exists? (contains? state key)
                                                matches? (if (nil? expected-etag)
                                                           (not exists?)
                                                           (= expected-etag old-meta))]
                                            (if matches?
                                              (assoc state key [(meta-update key :edn old-meta) new-value])
                                              state))))
                            [old-meta _old-value] (get old-state key)
                            existed? (contains? old-state key)
                            won? (if (nil? expected-etag)
                                   (not existed?)
                                   (= expected-etag old-meta))]
                        (if won?
                          (do
                            (when-let [cache-atom (:cache store)]
                              (swap! cache-atom cache/evict key)
                              (swap! cache-atom cache/miss key new-value))
                            :ok)
                          :conflict))))))))

(defn- tx-report-modified-attrs [tx-report]
  (let [rim (:ref-ident-map (:db-after tx-report))]
    (into #{}
          (comp (map :a)
                (filter some?)
                (map (fn [a]
                       (if (and rim (number? a))
                         (get rim a a)
                         a))))
          (:tx-data tx-report))))

(defn replay-wal-tx [db {:keys [tx-data tx-meta max-tx max-eid]}]
  (let [expected {:max-tx max-tx
                  :max-eid max-eid}
        tx-report (dbt/replay-concrete-tx-data db (mapv wal->datom tx-data) tx-meta expected)
        tx-report (assoc-in tx-report [:db-after :meta :datahike/updated-at]
                             (:db/txInstant tx-meta))]
    (dq/propagate-query-cache db (:db-after tx-report) (tx-report-modified-attrs tx-report))
    tx-report))

(defn replay-wal-entry [db wal-entry]
  (let [db-after (reduce (fn [db tx]
                           (:db-after (replay-wal-tx db tx)))
                         db
                         (:datahike/txs wal-entry))
        db-after (assoc-in db-after [:meta :datahike/commit-id]
                           (:datahike/wal-id wal-entry))
        expected (update (:datahike/db-after wal-entry) :meta wal-summary-meta)
        actual (db-summary db-after)]
    (when-not (= expected actual)
      (log/raise "Replayed WAL entry summary does not match expected summary."
                 {:type :remote-wal/replay-summary-mismatch
                  :wal-id (:datahike/wal-id wal-entry)
                  :expected expected
                  :actual actual}))
    db-after))

(defn replay-wal-entries [db wal-entries]
  (reduce replay-wal-entry db wal-entries))

(defn pending-wal-suffix
  "Return the pending entries that need replay after `commit-id`."
  [wal-record commit-id]
  (let [pending (vec (:datahike/pending wal-record))]
    (cond
      (= commit-id (:datahike/wal-head wal-record)) []
      (= commit-id (:datahike/materialized-head wal-record)) pending
      (nil? commit-id) pending
      :else (if-let [idx (first (keep-indexed (fn [idx entry]
                                                (when (= commit-id (:datahike/wal-id entry)) idx))
                                              pending))]
              (subvec pending (inc idx))
              (log/raise "Local DB commit is not on the remote WAL branch."
                         {:type :remote-wal/local-head-mismatch
                          :commit-id commit-id
                          :wal-head (:datahike/wal-head wal-record)
                          :materialized-head (:datahike/materialized-head wal-record)})))))

(defn sync-db-to-wal-record [db wal-record]
  (let [commit-id (get-in db [:meta :datahike/commit-id])]
    (replay-wal-entries db (pending-wal-suffix wal-record commit-id))))

(defn empty-remote-wal-db [config store]
  (assoc (db/empty-db nil config store) :store store))

(defn- indexed-attrs-for-config [db config]
  (if (:attribute-refs? config)
    (set (keep (:ident-ref-map db) (get-in db [:rschema :db/index])))
    (get-in db [:rschema :db/index])))

(defn rebuild-db-indexes-for-store
  "Rebuild primary indexes from logical datoms so `db` can use `store`.

  This is used when a remote materialized checkpoint was read from one store
  but the live connection must continue on the local cache store. Reusing the
  deserialized index roots directly would keep their internal storage bound to
  the source store; rebuilding gives future speculative writes and local
  materialization attempt-local pending writes on the target store."
  [db store config]
  (let [current-datoms (vec (dbi/datoms db :eavt []))
        temporal-datoms (when (:keep-history? config)
                          (vec (dbs/search-temporal-indices db nil)))
        index (:index config)
        index-config (merge (:index-config config)
                            {:indexed (indexed-attrs-for-config db config)})
        rebuilt (assoc db
                       :store store
                       :config config
                       :eavt (di/init-index index store current-datoms :eavt 0 index-config)
                       :aevt (di/init-index index store current-datoms :aevt 0 index-config)
                       :avet (di/init-index index store current-datoms :avet 0 index-config))]
    (if (:keep-history? config)
      (assoc rebuilt
             :temporal-eavt (di/init-index index store temporal-datoms :eavt 0 index-config)
             :temporal-aevt (di/init-index index store temporal-datoms :aevt 0 index-config)
             :temporal-avet (di/init-index index store temporal-datoms :avet 0 index-config))
      rebuilt)))

(defn- ensure-db-store [db store config]
  (if (identical? (:store db) store)
    (assoc db :config config)
    (rebuild-db-indexes-for-store db store config)))

(defn reconstruct-db-from-wal [config local-store remote-store wal-record local-stored-db]
  (let [wal-head (:datahike/wal-head wal-record)
        local-cid (get-in local-stored-db [:meta :datahike/commit-id])
        db (cond
             (and local-stored-db (= local-cid wal-head))
             (stored->db (assoc local-stored-db :config config) local-store)

             (and local-stored-db (= local-cid (:datahike/materialized-head wal-record)))
             (stored->db (assoc local-stored-db :config config) local-store)

             (:datahike/materialized-db wal-record)
             (stored->db (assoc (:datahike/materialized-db wal-record) :config config)
                         remote-store)

             :else
             (empty-remote-wal-db config local-store))]
    (ensure-db-store (sync-db-to-wal-record db wal-record) local-store config)))

(defn- wal-record-can-advance-db?
  "Return true when `db`'s commit id is still represented in `wal-record`.

  A nil commit id is only replayable from pending before the first remote
  materialized checkpoint. Once pending has been compacted, a nil or older
  local DB must reload the remote materialized DB rather than replaying the
  remaining pending suffix on top of an empty/stale base."
  [wal-record commit-id]
  (cond
    (= commit-id (:datahike/wal-head wal-record)) true
    (= commit-id (:datahike/materialized-head wal-record)) true
    (nil? commit-id) (nil? (:datahike/materialized-head wal-record))
    :else (boolean (some #(= commit-id (:datahike/wal-id %))
                         (:datahike/pending wal-record)))))

(defn sync-db-to-wal-record-or-materialized
  "Advance `db` to `wal-record`, reloading the remote materialized checkpoint
  when pending WAL no longer contains the local commit.

  This is used by live writers that can stay open while another process
  materializes and clears WAL entries. Startup uses `reconstruct-db-from-wal`
  directly because it may also consider a local stored snapshot."
  [db remote-store wal-record]
  (let [commit-id (get-in db [:meta :datahike/commit-id])]
    (if (wal-record-can-advance-db? wal-record commit-id)
      (sync-db-to-wal-record db wal-record)
      (if (:datahike/materialized-db wal-record)
        (reconstruct-db-from-wal (:config db) (:store db) remote-store wal-record nil)
        (sync-db-to-wal-record db wal-record)))))

(defn get-and-clear-pending-kvs!
  "Retrieves and clears pending key-value pairs from the store's pending-writes atom.
  Assumes :pending-writes in store's storage holds an atom of a collection of [key value] pairs."
  [store]
  (let [pending-writes-atom (-> store :storage :pending-writes) ; Assumes :storage key holds the CachedStorage
        kvs-to-write (atom [])]
    (when pending-writes-atom
      ;; Atomically get current KVs and reset the pending-writes atom.
      (swap! pending-writes-atom (fn [old-kvs] (reset! kvs-to-write old-kvs) [])))
    @kvs-to-write))

(defn db->stored
  "Maps memory db to storage layout. Index flushes will add [k v] pairs to pending-writes."
  [db flush?]
  (when-not (dbu/db? db)
    (log/raise "Argument is not a database."
               {:type     :argument-is-not-a-db
                :argument db}))
  (let [{:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                schema rschema system-entities ident-ref-map ref-ident-map config
                max-tx max-eid op-count hash meta store]} db
        schema-meta {:schema schema
                     :rschema rschema
                     :system-entities system-entities
                     :ident-ref-map ident-ref-map
                     :ref-ident-map ref-ident-map}
        schema-meta-key (uuid schema-meta)
        backend                                           (di/konserve-backend (:index config) store)
        not-in-memory?                                    (not= :memory (-> config :store :backend))
        flush! (and flush? not-in-memory?)
        ;; Prepare schema meta KV pair for writing, but don't write it here.
        schema-meta-kv-to-write (when-not (sc/write-cache-has? (:store config) schema-meta-key)
                                  (sc/add-to-write-cache (:store config) schema-meta-key)
                                  [schema-meta-key schema-meta])]
    (when-not (sc/cache-has? schema-meta-key)
      (sc/cache-miss schema-meta-key schema-meta))
    (let [;; Flush primary indices, capturing the post-flush instances so
          ;; we can both serialize their storage keys and ask each for a
          ;; merkle-root via the IAuditable protocol.
          eavt'          (cond-> eavt flush! (di/-flush backend))
          aevt'          (cond-> aevt flush! (di/-flush backend))
          avet'          (cond-> avet flush! (di/-flush backend))
          temporal-eavt' (when (:keep-history? config)
                           (cond-> temporal-eavt flush! (di/-flush backend)))
          temporal-aevt' (when (:keep-history? config)
                           (cond-> temporal-aevt flush! (di/-flush backend)))
          temporal-avet' (when (:keep-history? config)
                           (cond-> temporal-avet flush! (di/-flush backend)))
          ;; Secondary indices manage their own storage (Lucene files,
          ;; konserve, mmap) so they must always be flushed regardless of
          ;; the primary store backend.
          secondary-index-keys
          #?(:clj
             (when (and flush? (seq (:secondary-indices db)))
               (reduce-kv
                (fn [acc idx-ident idx]
                  (if (satisfies? sec/IVersionedSecondaryIndex idx)
                    (assoc acc idx-ident (sec/-sec-flush idx store (:branch config)))
                    acc))
                {} (:secondary-indices db)))
             :cljs nil)
          ;; Audit roots: per-index content-addressed UUIDs that feed
          ;; into the commit-id via merkle-leaves.
          ;;
          ;; Primary indexes implement IAuditable: their flushed instance
          ;; carries the merkle root (e.g. PSS `_address` is post-flush).
          ;;
          ;; Secondary indexes can produce their merkle root in two
          ;; ways: (a) extend IAuditable when their live instance has
          ;; post-flush state visible to the bridge — scriptum, whose
          ;; underlying Java writer is mutable so `(.getLastContentHash
          ;; bw)` reflects the latest commit on the same handle; (b)
          ;; surface `:merkle-root` in their -sec-flush return map when
          ;; sync produces a new immutable value the bridge field
          ;; doesn't capture — stratum and proximum, whose record-typed
          ;; live values stay pinned to the pre-sync state. The reader
          ;; below tries (a) first, then (b).
          safe-root      (fn [x]
                           (when x
                             (try (audit/-merkle-root x)
                                  (catch #?(:clj Throwable :cljs js/Error) _ nil))))
          sec-roots      (when (seq (:secondary-indices db))
                           (reduce-kv
                            (fn [acc idx-ident idx]
                              (assoc acc idx-ident
                                     (or (safe-root idx)
                                         (:merkle-root (get secondary-index-keys idx-ident)))))
                            {} (:secondary-indices db)))
          merkle-roots
          (cond-> {:eavt-key (safe-root eavt')
                   :aevt-key (safe-root aevt')
                   :avet-key (safe-root avet')}
            (:keep-history? config)
            (assoc :temporal-eavt-key (safe-root temporal-eavt')
                   :temporal-aevt-key (safe-root temporal-aevt')
                   :temporal-avet-key (safe-root temporal-avet'))
            sec-roots
            (assoc :secondary sec-roots))]
      [schema-meta-kv-to-write
       (merge
        {:schema-meta-key  schema-meta-key
         :config          config
         :meta            meta
         :hash            hash
         :max-tx          max-tx
         :max-eid         max-eid
         :op-count        op-count
         :merkle-roots    merkle-roots
         :eavt-key        eavt'
         :aevt-key        aevt'
         :avet-key        avet'}
        (when (:keep-history? config)
          {:temporal-eavt-key temporal-eavt'
           :temporal-aevt-key temporal-aevt'
           :temporal-avet-key temporal-avet'})
        (when secondary-index-keys
          {:secondary-index-keys secondary-index-keys}))])))

(defn- restore-secondary-indices
  "Restore secondary index instances from stored key-maps.
   For versioned indices (IVersionedSecondaryIndex), restores from durable storage.
   For non-versioned or missing keys, creates empty instances that need backfill."
  [schema ident-ref-map secondary-index-keys store]
  #?(:clj
     (reduce-kv
      (fn [acc ident entry]
        (if (and (map? entry) (:db.secondary/type entry))
          (let [idx-type (:db.secondary/type entry)
                idx-attrs (set (:db.secondary/attrs entry))
                key-map (get secondary-index-keys ident)
                idx-config (cond-> (merge (:db.secondary/config entry)
                                          {:attrs idx-attrs})
                             (seq ident-ref-map)
                             (assoc :ident-ref-map ident-ref-map)
                             ;; When a key-map carries a branch, route the
                             ;; skeleton into that branch too — otherwise
                             ;; the factory defaults to "main" and a non-
                             ;; main connection re-opens the main writer,
                             ;; contending for its per-branch lock.
                             (:branch key-map)
                             (assoc :branch (:branch key-map)))]
            (try
              (let [skeleton (sec/create-index idx-type idx-config nil)]
                (if (and key-map (satisfies? sec/IVersionedSecondaryIndex skeleton))
                  ;; Restore from durable storage. The skeleton existed
                  ;; only to satisfy the protocol check; close its native
                  ;; resources (e.g. Lucene's per-branch write lock)
                  ;; before `-sec-restore` opens its own writer at the
                  ;; same path/branch — otherwise the two contend.
                  (do (when (instance? java.io.Closeable skeleton)
                        (try (.close ^java.io.Closeable skeleton)
                             (catch Exception _)))
                      (assoc acc ident (sec/-sec-restore skeleton store key-map)))
                  ;; No stored keys — empty index, needs backfill
                  (assoc acc ident skeleton)))
              (catch Exception e
                (log/warn :datahike/secondary-index-restore-failed {:ident ident :error (.getMessage e)})
                acc)))
          acc))
      {} schema)
     :cljs {}))

(defn stored->db
  "Constructs in-memory db instance from stored map value."
  [stored-db store]
  (let [{:keys [eavt-key aevt-key avet-key
                temporal-eavt-key temporal-aevt-key temporal-avet-key
                secondary-index-keys
                schema rschema system-entities ref-ident-map ident-ref-map
                config max-tx max-eid op-count hash meta schema-meta-key]
         :or   {op-count 0}} stored-db
        schema-meta (or (sc/cache-lookup schema-meta-key)
                        ;; not in store in case we load an old db where the schema meta data was inline
                        (when-let [schema-meta (k/get store schema-meta-key nil {:sync? true})]
                          (sc/cache-miss schema-meta-key schema-meta)
                          schema-meta))
        effective-schema (or (:schema schema-meta) schema)
        effective-ident-ref-map (or (:ident-ref-map schema-meta) ident-ref-map)
        sec-indices (restore-secondary-indices effective-schema effective-ident-ref-map
                                               secondary-index-keys store)
        empty       (db/empty-db nil config store)]
    (merge
     (assoc empty
            :max-tx max-tx
            :max-eid max-eid
            :config config
            :meta meta
            :schema schema
            :hash hash
            :op-count op-count
            :eavt eavt-key
            :aevt aevt-key
            :avet avet-key
            :temporal-eavt temporal-eavt-key
            :temporal-aevt temporal-aevt-key
            :temporal-avet temporal-avet-key
            :rschema rschema
            :system-entities system-entities
            :ident-ref-map ident-ref-map
            :ref-ident-map ref-ident-map
            :store store)
     (when (seq sec-indices)
       {:secondary-indices sec-indices})
     schema-meta)))

(defn branch-heads-as-commits [store parents]
  (set (doall (for [p parents]
                (do
                  (when (nil? p)
                    (log/raise "Parent cannot be nil." {:type :parent-cannot-be-nil
                                                        :parent p}))
                  (if-not (keyword? p) p
                          (let [{{:keys [datahike/commit-id]} :meta :as old-db}
                                (k/get store p nil {:sync? true})]
                            (when-not old-db
                              (log/raise "Parent does not exist in store."
                                         {:type   :parent-does-not-exist-in-store
                                          :parent p}))
                            commit-id)))))))

(defn- audit-grade?
  "Audit-grade cids require :crypto-hash? on a persistent backend,
   plus a `:merkle-roots` map computed during `db->stored` whose
   primary entries (eavt-key/aevt-key/avet-key) are non-nil — i.e.
   the primary index impl extends `IAuditable`."
  [config stored-db]
  (and (:crypto-hash? config)
       (not= :memory (get-in config [:store :backend]))
       (some? stored-db)
       (every? some?
               (vals (select-keys (:merkle-roots stored-db)
                                  [:eavt-key :aevt-key :avet-key])))))

(defn create-commit-id
  "Compute the commit-id for `db`.

   In audit-grade mode, returns a content-addressed UUID-5 over the
   stored `:merkle-roots` map + schema-meta-key + max-tx + max-eid +
   meta. Otherwise falls back to `[hash max-tx max-eid meta]`, wrapped
   in `squuid` when `:crypto-hash?` is off."
  ([db] (create-commit-id db nil))
  ([db stored-db]
   (let [{:keys [hash max-tx max-eid meta config]} db
         content (if (audit-grade? config stored-db)
                   [(:merkle-roots stored-db)
                    (:schema-meta-key stored-db)
                    max-tx max-eid
                    (dissoc meta :datahike/commit-id)]
                   [hash max-tx max-eid meta])
         content-uuid (uuid content)]
     (if (:crypto-hash? config)
       content-uuid
       (squuid content-uuid)))))

(defn write-pending-kvs!
  "Writes a collection of key-value pairs to the store.
  Handles synchronous and asynchronous writes.
  Assumes it's called within a go-try- block if sync? is false."
  [store kvs sync?]
  (if sync?
    (doseq [[k v] kvs]
      (k/assoc store k v {:sync? true}))
    (let [pending-ops (mapv (fn [[k v]] (k/assoc store k v {:sync? false})) kvs)]
      (go-try- (doseq [op pending-ops] (<?- op))))))

(defn commit!
  ([db parents]
   (commit! db parents true))
  ([db parents sync?]
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [{:keys [store config]} db
                      parents       (or parents #{(get config :branch)})
                      parents       (branch-heads-as-commits store parents)
                      ;; Stamp parents BEFORE flushing so they're in the
                      ;; stored form the cid will be derived from.
                      db            (assoc-in db [:meta :datahike/parents] parents)
                      ;; Flush first → cid sees post-flush storage
                      ;; addresses (true merkle leaves under crypto-hash?).
                      [schema-meta-kv-to-write db-to-store-pre]
                      (db->stored db true)
                      cid           (create-commit-id db db-to-store-pre)
                      db            (assoc-in db [:meta :datahike/commit-id] cid)
                      db-to-store   (assoc-in db-to-store-pre
                                              [:meta :datahike/commit-id] cid)
                      pending-kvs   (get-and-clear-pending-kvs! store)]

                  (if (multi-key-capable? store)
                    (let [[meta-key meta-val] schema-meta-kv-to-write
                          writes-map (cond-> (into {} pending-kvs) ; Initialize with pending KVs
                                       schema-meta-kv-to-write (assoc meta-key meta-val)
                                       true                    (assoc cid db-to-store)
                                       true                    (assoc (:branch config) db-to-store))]
                      (<?- (k/multi-assoc store writes-map {:sync? sync?})))
                    ;; Then write schema-meta, commit-log, branch
                    (let [[meta-key meta-val] schema-meta-kv-to-write
                          schema-meta-written (when schema-meta-kv-to-write
                                                (k/assoc store meta-key meta-val {:sync? sync?}))

                          ;; Make sure all pointed to values are written before the commit log and branch
                          _ (when schema-meta-kv-to-write (<?- schema-meta-written))
                          _ (<?- (write-pending-kvs! store pending-kvs sync?))

                          commit-log-written (k/assoc store cid db-to-store {:sync? sync?})
                          branch-written     (k/assoc store (:branch config) db-to-store {:sync? sync?})]
                      (when-not sync?
                        (<?- commit-log-written)
                        (<?- branch-written))))

                  ;; Online GC: delete freed addresses after writes are committed
                  (when (get-in config [:online-gc :enabled?])
                    (<?- (online-gc/online-gc! store (assoc (:online-gc config) :sync? false))))

                  db)))))

(defn materialize-db-with-cid!
  "Flush `db` to its configured store using the supplied commit id.

  This is intentionally separate from `commit!`: remote-WAL mode has already
  established the durable commit id with the remote WAL CAS and must not derive
  a new index-content commit id while materializing a cache snapshot."
  ([db wal-cid]
   (materialize-db-with-cid! db wal-cid {}))
  ([db wal-cid {:keys [store store-config branch sync? write-branch?]
                :or {sync? true write-branch? true}}]
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [store (or store (:store db))
                      source-store (:store db)
                      config (cond-> (:config db)
                               store-config (assoc :store store-config)
                               branch (assoc :branch branch))
                      branch (when write-branch?
                               (or branch (:branch config)))
                      db (-> db
                             (assoc :store store :config config)
                             (assoc-in [:meta :datahike/commit-id] wal-cid))
                      db (if (and source-store (not (identical? source-store store)))
                           (rebuild-db-indexes-for-store db store config)
                           db)
                      [schema-meta-kv-to-write db-to-store-pre] (db->stored db true)
                      db-to-store (assoc-in db-to-store-pre [:meta :datahike/commit-id] wal-cid)
                      pending-kvs (get-and-clear-pending-kvs! store)]
                  (if (multi-key-capable? store)
                    (let [[meta-key meta-val] schema-meta-kv-to-write
                          writes-map (cond-> (into {} pending-kvs)
                                       schema-meta-kv-to-write (assoc meta-key meta-val)
                                       true (assoc wal-cid db-to-store)
                                       branch (assoc branch db-to-store))]
                      (<?- (k/multi-assoc store writes-map {:sync? sync?})))
                    (let [[meta-key meta-val] schema-meta-kv-to-write]
                      (when schema-meta-kv-to-write
                        (<?- (k/assoc store meta-key meta-val {:sync? sync?})))
                      (<?- (write-pending-kvs! store pending-kvs sync?))
                      (<?- (k/assoc store wal-cid db-to-store {:sync? sync?}))
                      (when branch
                        (<?- (k/assoc store branch db-to-store {:sync? sync?})))))
                  db)))))

(defn remote-materialize-wal!
  "Materialize the current remote WAL pending prefix into the remote store.

  The WAL head object remains the only branch object at `wal-key`; the
  Datahike stored DB snapshot is embedded back into the WAL record after all
  referenced index KVs and the commit-log entry are durable. If concurrent
  commits or another materializer changes the WAL head, this retries from the
  latest head up to [:writer :wal-max-retries]."
  ([remote-store wal-key config]
   (remote-materialize-wal! remote-store wal-key config {:sync? true}))
  ([remote-store wal-key config opts]
   (let [opts (merge {:sync? true} opts)]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (let [max-retries (get-in config [:writer :wal-max-retries] 10)
                        remote-store-config (or (get-in config [:writer :remote-store])
                                                (:store config))
                        remote-config (assoc config :store remote-store-config)]
                    (loop [attempt 0]
                    (let [wal-read (<?- (get-wal-head-with-etag remote-store wal-key opts))]
                      (when-not (:exists? wal-read)
                        (log/raise "Remote WAL head does not exist."
                                   {:type :remote-wal/head-missing
                                    :wal-key wal-key}))
                      (when-not (remote-wal-record? (:value wal-read) wal-key)
                        (log/raise "Remote WAL head object is not a Datahike remote WAL record for this branch."
                                   {:type :remote-wal/invalid-head
                                    :wal-key wal-key
                                    :expected-branch wal-key
                                    :actual-branch (:datahike/branch (:value wal-read))
                                    :value (:value wal-read)}))
                      (let [wal-record (:value wal-read)
                            pending (vec (:datahike/pending wal-record))]
                        (if (empty? pending)
                          wal-record
                          (let [base-db (if-let [stored-db (:datahike/materialized-db wal-record)]
                                          (stored->db (assoc stored-db :config remote-config)
                                                      remote-store)
                                          (empty-remote-wal-db remote-config remote-store))
                                target-head (:datahike/wal-id (peek pending))
                                db-after (replay-wal-entries base-db pending)
                                _ (when-not (= target-head (get-in db-after [:meta :datahike/commit-id]))
                                    (log/raise "Remote WAL materialization did not replay to the expected head."
                                               {:type :remote-wal/materialization-head-mismatch
                                                :expected target-head
                                                :actual (get-in db-after [:meta :datahike/commit-id])}))
                                _ (<?- (materialize-db-with-cid! db-after target-head
                                                                 {:store remote-store
                                                                  :store-config remote-store-config
                                                                  :sync? (:sync? opts)
                                                                  :write-branch? false}))
                                stored-db (<?- (k/get remote-store target-head nil opts))
                                _ (when-not (stored-db? stored-db)
                                    (log/raise "Remote WAL materialization did not write a stored DB snapshot."
                                               {:type :remote-wal/materialization-missing-stored-db
                                                :wal-head target-head}))
                                fresh-read (<?- (get-wal-head-with-etag remote-store wal-key opts))
                                fresh-record (:value fresh-read)
                                fresh-pending (vec (:datahike/pending fresh-record))
                                materialized-count (count pending)
                                expected-prefix (mapv :datahike/wal-id pending)
                                actual-prefix (mapv :datahike/wal-id
                                                    (take materialized-count fresh-pending))]
                            (if (= expected-prefix actual-prefix)
                              (let [updated-record (assoc fresh-record
                                                          :datahike/materialized-head target-head
                                                          :datahike/materialized-db stored-db
                                                          :datahike/pending (subvec fresh-pending materialized-count))
                                    cas-result (<?- (cas-assoc! remote-store wal-key (:etag fresh-read)
                                                                updated-record opts))]
                                (case cas-result
                                  :ok updated-record
                                  :conflict (if (< attempt max-retries)
                                              (recur (inc attempt))
                                              (log/raise "Remote WAL materialization CAS failed after maximum retries."
                                                         {:type :remote-wal/materialization-cas-retries-exhausted
                                                          :attempts (inc attempt)
                                                          :wal-key wal-key}))
                                  (log/raise "Remote WAL CAS helper returned an invalid result."
                                             {:type :remote-wal/invalid-cas-result
                                              :result cas-result
                                              :wal-key wal-key})))
                              (if (< attempt max-retries)
                                (recur (inc attempt))
                                (log/raise "Remote WAL pending entries changed before materialization could advance."
                                           {:type :remote-wal/materialization-prefix-mismatch
                                            :expected expected-prefix
                                            :actual actual-prefix
                                            :wal-key wal-key}))))))))))))))

(defn complete-db-update [old tx-report]
  (let [{:keys [writer]} old
        {:keys [db-after tx-data]
         {:keys [db/txInstant]} :tx-meta} tx-report
        new-meta  (assoc (:meta db-after) :datahike/updated-at txInstant)
        db        (cond-> (assoc db-after :meta new-meta :writer writer)
                    (:store old)
                    (assoc :store (:store old))
                    (:remote-wal-store old)
                    (assoc :remote-wal-store (:remote-wal-store old)
                           :remote-wal-store-config (:remote-wal-store-config old)))
        ;; Propagate query result cache from old DB to new DB.
        _ (dq/propagate-query-cache old db (tx-report-modified-attrs tx-report))
        tx-report (assoc tx-report :db-after db)]
    tx-report))

(defprotocol PDatabaseManager
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(defn -database-exists?* [config]
  (let [p (dt/throwable-promise)]
    (go-try-
     (put! p (try
               (let [config (dc/load-config config)
                     opts {:sync? false}
                     remote-wal? (dc/remote-wal-config? config)
                     store-config (if remote-wal?
                                    (get-in config [:writer :remote-store])
                                    (:store config))
                     branch (if remote-wal?
                              (get-in config [:writer :wal-branch])
                              :db)
                     exists? (if remote-wal?
                               #(remote-wal-record? % branch)
                               some?)
                     store-exists? (<?- (ks/store-exists? store-config opts))]
                 (if store-exists?
                   (let [raw-store (<?- (ks/connect-store store-config opts))
                         store (ds/add-cache-and-handlers raw-store (assoc config :store store-config))
                         _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                         stored-value (<?- (k/get store branch nil opts))]
                     (<?- (ks/release-store store-config store opts))
                     (boolean (exists? stored-value)))
                   false))
               (catch #?(:clj Throwable :cljs js/Error) e
                 e))))
    p))

(defn -create-database* [config deprecated-config]
  (go-try-
   (let [opts {:sync? false}
         {:keys [keep-history?] :as config} (dc/load-config config deprecated-config)
         store-config (:store config)
         store (ds/add-cache-and-handlers (<?- (ks/create-store store-config opts)) config)
         stored-db (<?- (k/get store :db nil opts))
         _ (when stored-db
             (log/raise "Database already exists."
                        {:type :db-already-exists :config store-config}))
         {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                 schema rschema system-entities ref-ident-map ident-ref-map
                 config max-tx max-eid op-count hash meta] :as db}
         (db/empty-db nil config store)
         backend (di/konserve-backend (:index config) store)
         schema-meta {:schema schema
                      :rschema rschema
                      :system-entities system-entities
                      :ident-ref-map ident-ref-map
                      :ref-ident-map ref-ident-map}
         schema-meta-key (uuid schema-meta)
         ;; Flush first → cid sees post-flush storage addresses.
         eavt'          (di/-flush eavt backend)
         aevt'          (di/-flush aevt backend)
         avet'          (di/-flush avet backend)
         temporal-eavt' (when keep-history? (di/-flush temporal-eavt backend))
         temporal-aevt' (when keep-history? (di/-flush temporal-aevt backend))
         temporal-avet' (when keep-history? (di/-flush temporal-avet backend))
         safe-root      (fn [x]
                          (when x
                            (try (audit/-merkle-root x)
                                 (catch Throwable _ nil))))
         merkle-roots   (cond-> {:eavt-key (safe-root eavt')
                                 :aevt-key (safe-root aevt')
                                 :avet-key (safe-root avet')}
                          keep-history?
                          (assoc :temporal-eavt-key (safe-root temporal-eavt')
                                 :temporal-aevt-key (safe-root temporal-aevt')
                                 :temporal-avet-key (safe-root temporal-avet')))
         pre-cid-stored
         (merge {:max-tx          max-tx
                 :max-eid         max-eid
                 :op-count        op-count
                 :hash            hash
                 :merkle-roots    merkle-roots
                 :schema-meta-key schema-meta-key
                 :config          (update config :initial-tx (comp not empty?))
                 :meta            meta
                 :eavt-key        eavt'
                 :aevt-key        aevt'
                 :avet-key        avet'}
                (when keep-history?
                  {:temporal-eavt-key temporal-eavt'
                   :temporal-aevt-key temporal-aevt'
                   :temporal-avet-key temporal-avet'}))
         cid (create-commit-id db pre-cid-stored)
         meta (assoc meta :datahike/commit-id cid)
         db-to-store (assoc pre-cid-stored :meta meta)]
     ;;we just created the first data base in this store, so the write cache is empty
     (<?- (k/assoc store schema-meta-key schema-meta opts))
     (sc/add-to-write-cache (:store config) schema-meta-key)
     (when-not (sc/cache-has? schema-meta-key)
       (sc/cache-miss schema-meta-key schema-meta))

     ;; Process pending KVs from index flushes synchronously
     (let [pending-kvs (get-and-clear-pending-kvs! store)]
       (<?- (write-pending-kvs! store pending-kvs false)))

     (<?- (k/assoc store :branches #{:db} opts))
     (<?- (k/assoc store cid db-to-store opts))
     (<?- (k/assoc store :db db-to-store opts))
     (ks/release-store store-config store)
     config)))

(defn -delete-database* [config]
  (go-try-
   (let [config (dc/load-config config {})
         config-store-id (ds/store-identity (:store config))
         active-conns (filter (fn [[store-id _branch]]
                                (= store-id config-store-id))
                              (keys @*connections*))]
     (sc/clear-write-cache (:store config))
     (doseq [conn active-conns]
       (log/warn :datahike/delete-unreleased-connections {:connection conn})
       (delete-connection! conn))
     (ks/delete-store (:store config)))))

(extend-protocol PDatabaseManager
  #?(:clj String :cljs string)
  (-create-database #?(:clj [uri & opts] :cljs [uri opts])
    (-create-database (dc/uri->config uri) opts))

  (-delete-database [uri]
    (-delete-database (dc/uri->config uri)))

  (-database-exists? [uri]
    (-database-exists? (dc/uri->config uri)))

  #?(:clj clojure.lang.IPersistentMap :cljs PersistentArrayMap)
  (-database-exists? [config]
    (-database-exists?* config))
  (-create-database [config opts]
    (-create-database* config opts))
  (-delete-database [config]
    (-delete-database* config))

  #?(:cljs PersistentHashMap)
  #?(:cljs
     (-database-exists? [config]
                        (-database-exists?* config)))
  #?(:cljs (-create-database [config opts] (-create-database* config opts)))
  #?(:cljs (-delete-database [config] (-delete-database* config))))

;; public API

(defn create-database
  ([]
   (-create-database {} nil))
  ([config & opts]
   (-create-database config opts)))

(defn delete-database
  ([]
   (-delete-database {}))
  ;;deprecated
  ([config]
   ;; TODO log deprecation notice with #54
   (-delete-database config)))

(defn database-exists?
  ([]
   (-database-exists? {}))
  ([config]
   ;; TODO log deprecation notice with #54
   (-database-exists? config)))

#?(:clj
   (defn build-secondary-index!
     "Backfill a secondary index by scanning AEVT for all covered attributes.
      Returns a channel (async op) so the writer continues processing other
      transactions during backfill. When complete, dispatches a lightweight
      install-secondary-index! op to atomically swap in the result."
     [old idx-ident]
     (log/trace :datahike/build-secondary-index {:idx-ident idx-ident})
     ;; Return a channel — writer runs this in background (lines 89-93 of writer.cljc)
     (let [db old
           idx (get-in db [:secondary-indices idx-ident])
           _ (when-not idx
               (log/raise "Secondary index not found" {:idx-ident idx-ident}))
           attrs (sec/-indexed-attrs idx)
           building-since-tx (get-in db [:schema idx-ident :db.secondary/building-since-tx])
           use-transient? (satisfies? sec/ITransientSecondaryIndex idx)
           t-idx (if use-transient? (sec/-as-transient idx) idx)]
       ;; Background go block — doesn't block the writer
       (go-try-
        (let [populated-idx
              (reduce
               (fn [current-idx attr]
                 (let [datoms (dbi/datoms db :aevt [attr])
                       n (atom 0)]
                   (log/debug :datahike/backfilling {:attr attr})
                   (let [result (reduce
                                 (fn [idx d]
                                   (if (and building-since-tx
                                            (> (.-tx ^datahike.datom.Datom d) building-since-tx))
                                     idx
                                     (do (swap! n inc)
                                         (let [tx-report {:datom d :added? true}]
                                           (if use-transient?
                                             (do (sec/-transact! idx tx-report) idx)
                                             (sec/-transact idx tx-report))))))
                                 current-idx datoms)]
                     (log/debug :datahike/backfilled {:attr attr :count @n})
                     result)))
               t-idx attrs)
              final-idx (if use-transient?
                          (sec/-persistent! populated-idx)
                          populated-idx)]
          (log/trace :datahike/secondary-index-built {:idx-ident idx-ident})
          ;; Return the populated index — the writer dispatch callback
          ;; receives this, but we need to install it via a separate writer op.
          ;; Store it in an atom for install-secondary-index! to pick up.
          {:idx-ident idx-ident :index final-idx})))))

#?(:clj
   (defn install-secondary-index!
     "Lightweight synchronous writer op that installs a backfilled index.
      Called after build-secondary-index! completes in the background."
     [old {:keys [idx-ident index]}]
     (let [db-after (-> old
                        (assoc-in [:secondary-indices idx-ident] index)
                        (assoc-in [:schema idx-ident :db.secondary/status] :ready)
                        (update-in [:schema idx-ident] dissoc :db.secondary/building-since-tx))]
       (complete-db-update old {:db-before old
                                :db-after db-after
                                :tx-data []
                                :tx-meta {}}))))

(defn merge-writer!
  "Writer operation for merge. Applies tx-data and records merge parents
   on the db meta so the commit loop creates a multi-parent merge commit."
  [old {:keys [parents tx-data tx-meta]}]
  (log/trace :datahike/merge {:parent-count (count parents) :tx-count (count tx-data)})
  (let [tx-report (complete-db-update old (core/with old tx-data tx-meta))
        ;; Add merge parents to db meta — commit loop picks these up
        branch (get-in old [:config :branch])
        all-parents (conj (set parents) branch)]
    (update tx-report :db-after
            assoc-in [:meta :datahike/merge-parents] all-parents)))

(defn transact! [old {:keys [tx-data tx-meta]}]
  (log/debug :datahike/transact {:tx-count (count tx-data)})
  (log/trace :datahike/transact-detail {:tx-data tx-data :tx-meta tx-meta})
  (complete-db-update old (core/with old tx-data tx-meta)))

(defn load-entities [old entities]
  (log/debug :datahike/load-entities {:entity-count (count entities)})
  (complete-db-update old (core/load-entities-with old entities nil)))
