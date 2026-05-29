(ns datahike.test.remote-wal-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.config :as dc]
            [datahike.index.secondary :as sec]
            [datahike.writing :as w]
            [konserve.core :as k]
            [konserve.store :as ks]))

(defn- uuid []
  (java.util.UUID/randomUUID))

(defonce _remote-wal-secondary-index
  (sec/register-index-type!
   :remote-wal-test/secondary
   (fn [config _db]
     (reify
       sec/ISecondaryIndex
       (-search [_ _ _] nil)
       (-estimate [_ _] 0)
       (-can-order? [_ _ _] false)
       (-slice-ordered [_ _ _ _ _ _] nil)
       (-indexed-attrs [_] (set (:attrs config)))
       (-transact [this _tx-report] this)))))

(defn- delete-store-quietly! [store-config]
  (try
    (ks/delete-store store-config {:sync? true})
    (catch Throwable _)))

(defn- ex-type [^Throwable e]
  (some (comp :type ex-data)
        (take-while some? (iterate #(.getCause ^Throwable %) e))))

(defn- remote-wal-config-with-store [store-config remote-id]
  {:store store-config
   :schema-flexibility :read
   :keep-history? true
   :writer {:backend :remote-wal
            :remote-store {:backend :memory :id remote-id}
            :wal-branch :db
            :wal-auto-materialize? false
            :wal-max-retries 3}})

(defn- remote-wal-config [local-id remote-id]
  (remote-wal-config-with-store {:backend :memory :id local-id} remote-id))

(defn- temp-file-store-config []
  (let [tmp-dir (java.nio.file.Files/createTempDirectory
                 "datahike-remote-wal-local-cache"
                 (make-array java.nio.file.attribute.FileAttribute 0))]
    {:backend :file
     :path (str (.resolve tmp-dir "store"))
     :id (uuid)}))

(defn- wait-until [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (pred) true
        (>= (System/currentTimeMillis) deadline) false
        :else (do
                (Thread/sleep 10)
                (recur))))))

(deftest remote-wal-config-validation
  (testing "remote WAL requires an explicit durable remote store"
    (let [cfg {:store {:backend :memory :id (uuid)}
               :writer {:backend :remote-wal}}]
      (try
        (dc/load-config cfg)
        (is false "expected remote WAL config validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/missing-remote-store (:type (ex-data e))))))))

  (testing "remote WAL remote-store must be a store config map"
    (let [cfg (assoc-in (remote-wal-config (uuid) (uuid))
                        [:writer :remote-store]
                        :not-a-store)]
      (try
        (dc/load-config cfg)
        (is false "expected remote WAL remote-store validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/invalid-remote-store (:type (ex-data e))))))))

  (testing "remote WAL only supports keyword WAL branches matching the Datahike branch"
    (let [invalid-wal-branch (assoc-in (remote-wal-config (uuid) (uuid))
                                       [:writer :wal-branch]
                                       "db")
          mismatched-branch (assoc (remote-wal-config (uuid) (uuid))
                                   :branch :other)]
      (try
        (dc/load-config invalid-wal-branch)
        (is false "expected remote WAL wal-branch validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/invalid-wal-branch (:type (ex-data e))))))
      (try
        (dc/load-config mismatched-branch)
        (is false "expected remote WAL branch validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/unsupported-branch (:type (ex-data e))))))))

  (testing "remote WAL rejects unsupported audit and online-GC modes"
    (let [crypto-cfg (assoc (remote-wal-config (uuid) (uuid))
                            :crypto-hash? true)
          online-gc-cfg (assoc (remote-wal-config (uuid) (uuid))
                               :online-gc {:enabled? true})]
      (try
        (dc/load-config crypto-cfg)
        (is false "expected remote WAL crypto-hash validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/unsupported-crypto-hash (:type (ex-data e))))))
      (try
        (dc/load-config online-gc-cfg)
        (is false "expected remote WAL online-GC validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/unsupported-online-gc (:type (ex-data e))))))))

  (testing "remote WAL rejects initial-tx instead of creating local-only seed data"
    (let [cfg (assoc (remote-wal-config (uuid) (uuid))
                     :initial-tx [{:db/id 1 :name "seed"}])]
      (try
        (dc/load-config cfg)
        (is false "expected remote WAL initial-tx validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/unsupported-initial-tx (:type (ex-data e))))))))

  (testing "remote WAL ignores empty initial-tx"
    (let [cfg (assoc (remote-wal-config (uuid) (uuid))
                     :initial-tx [])
          remote-store-config (get-in cfg [:writer :remote-store])
          remote-store (atom nil)]
      (try
        (is (not (contains? (dc/load-config cfg) :initial-tx)))
        (is (not (contains? (dc/load-config (assoc cfg :initial-tx "")) :initial-tx)))
        (d/create-database cfg)
        (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
        (is (empty? (:datahike/pending (k/get @remote-store :db nil {:sync? true})))
            "empty initial-tx must not append a no-op WAL entry")
        (finally
          (when @remote-store
            (ks/release-store remote-store-config @remote-store {:sync? true}))
          (delete-store-quietly! (:store cfg))
          (delete-store-quietly! remote-store-config)))))

  (testing "stores without explicit CAS support fail clearly"
    (try
      (w/cas-assoc! {} :db nil {})
      (is false "expected missing CAS helper to fail")
      (catch clojure.lang.ExceptionInfo e
        (is (= :remote-wal/cas-unavailable (:type (ex-data e)))))))

  (testing "non-S3 konserve default stores fail clearly"
    (let [tmp-dir (java.nio.file.Files/createTempDirectory
                   "datahike-remote-wal-cas"
                   (make-array java.nio.file.attribute.FileAttribute 0))
          store-config {:backend :file
                        :path (str (.resolve tmp-dir "store"))
                        :id (uuid)}
          store (atom nil)]
      (try
        (reset! store (ks/create-store store-config {:sync? true}))
        (try
          (w/cas-assoc! @store :db nil {})
          (is false "expected non-S3 default store CAS to fail")
          (catch clojure.lang.ExceptionInfo e
            (is (= :remote-wal/cas-unavailable (:type (ex-data e))))))
        (finally
          (when @store
            (ks/release-store store-config @store {:sync? true}))
          (delete-store-quietly! store-config))))))

(deftest remote-wal-database-exists-uses-remote-head
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])]
    (try
      (is (false? (d/database-exists? cfg)))
      (d/create-database cfg)
      (is (true? (d/database-exists? cfg)))
      (delete-store-quietly! (:store cfg))
      (is (true? (d/database-exists? cfg))
          "remote WAL database existence should not depend on the local cache")
      (delete-store-quietly! remote-store-config)
      (is (false? (d/database-exists? cfg)))
      (finally
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-create-uses-existing-remote-store-and-cleans-conflict
  (let [remote-id (uuid)
        local-id-a (uuid)
        local-id-b (uuid)
        cfg-a (remote-wal-config local-id-a remote-id)
        cfg-b (remote-wal-config local-id-b remote-id)
        remote-store-config (get-in cfg-a [:writer :remote-store])
        precreated-remote (atom nil)]
    (try
      ;; Tigris/S3 buckets commonly pre-exist; database creation should be
      ;; gated by the WAL head CAS, not by creating the backing store itself.
      (reset! precreated-remote (ks/create-store remote-store-config {:sync? true}))
      (ks/release-store remote-store-config @precreated-remote {:sync? true})
      (reset! precreated-remote nil)

      (d/create-database cfg-a)
      (is (true? (d/database-exists? cfg-a)))

      (try
        (d/create-database cfg-b)
        (is false "expected existing remote WAL head to reject create-database")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/database-already-exists (ex-type e)))))
      (is (false? (ks/store-exists? (:store cfg-b) {:sync? true}))
          "failed remote WAL creation should remove the just-created local cache")
      (is (true? (d/database-exists? cfg-a))
          "failed remote WAL creation must not remove the existing remote WAL")
      (finally
        (when @precreated-remote
          (ks/release-store remote-store-config @precreated-remote {:sync? true}))
        (delete-store-quietly! (:store cfg-a))
        (delete-store-quietly! (:store cfg-b))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-head-branch-mismatch-fails-clearly
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (let [wal-head (k/get @remote-store :db nil {:sync? true})]
        (k/assoc @remote-store :db (assoc wal-head :datahike/branch :other) {:sync? true}))
      (is (false? (d/database-exists? cfg))
          "a WAL head for a different branch is not this remote-WAL database")
      (try
        (d/connect cfg)
        (is false "expected remote WAL branch mismatch to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/invalid-head (:type (ex-data e))))
          (is (= :db (:expected-branch (ex-data e))))
          (is (= :other (:actual-branch (ex-data e))))))
      (finally
        (when @remote-store
          (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-delete-removes-head-without-dropping-remote-store
  (let [remote-id (uuid)
        local-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        remote-store (atom nil)]
    (try
      ;; A Tigris/S3 bucket may hold unrelated objects; deleting the Datahike
      ;; remote-WAL database should remove only the WAL head object.
      (reset! remote-store (ks/create-store remote-store-config {:sync? true}))
      (k/assoc @remote-store :sentinel :keep-me {:sync? true})
      (ks/release-store remote-store-config @remote-store {:sync? true})
      (reset! remote-store nil)

      (d/create-database cfg)
      (is (true? (d/database-exists? cfg)))
      (d/delete-database cfg)
      (is (false? (d/database-exists? cfg)))
      (is (true? (ks/store-exists? remote-store-config {:sync? true}))
          "remote-WAL delete should not delete the containing remote store")
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (is (= :keep-me (k/get @remote-store :sentinel nil {:sync? true})))
      (is (nil? (k/get @remote-store :db nil {:sync? true})))
      (finally
        (when @remote-store
          (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-existing-connection-checks-remote-store
  (let [shared-local-id (uuid)
        remote-id-a (uuid)
        remote-id-b (uuid)
        cfg-a (remote-wal-config shared-local-id remote-id-a)
        cfg-b-create (remote-wal-config (uuid) remote-id-b)
        cfg-b-same-local (remote-wal-config shared-local-id remote-id-b)
        remote-store-config-a (get-in cfg-a [:writer :remote-store])
        remote-store-config-b (get-in cfg-b-create [:writer :remote-store])
        conn (atom nil)]
    (try
      (d/create-database cfg-a)
      (d/create-database cfg-b-create)
      (reset! conn (d/connect cfg-a))
      (try
        (d/connect cfg-b-same-local)
        (is false "expected remote WAL connection cache mismatch")
        (catch clojure.lang.ExceptionInfo e
          (is (= :config-does-not-match-existing-connections
                 (:type (ex-data e))))))
      (finally
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg-a))
        (delete-store-quietly! (:store cfg-b-create))
        (delete-store-quietly! remote-store-config-a)
        (delete-store-quietly! remote-store-config-b)))))

(deftest remote-wal-unsupported-branch-ops-fail-clearly
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        expect-unsupported-branch-op
        (fn [f]
          (try
            (f)
            (is false "expected remote WAL branch operation to fail")
            (catch clojure.lang.ExceptionInfo e
              (is (= :remote-wal/unsupported-branch-operation
                     (:type (ex-data e)))))))]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (expect-unsupported-branch-op #(d/branches @conn))
      (expect-unsupported-branch-op #(d/branch! @conn :db :scratch))
      (expect-unsupported-branch-op #(d/delete-branch! @conn :scratch))
      (expect-unsupported-branch-op #(d/force-branch! @@conn :scratch #{nil}))
      (expect-unsupported-branch-op #(d/merge-db @conn #{:db} []))
      (expect-unsupported-branch-op #(d/branch-as-db @conn :db))
      (finally
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-transaction-functions-fail-with-auto-retry
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (try
        (d/transact @conn [[:db.fn/call (fn [_db]
                                          [[:db/add 1 :name "unsafe"]])]])
        (is false "expected remote WAL transaction function guard to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/unsupported-transaction-functions
                 (ex-type e)))))
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (is (empty? (:datahike/pending (k/get @remote-store :db nil {:sync? true})))
          "guarded transaction functions must not append to the remote WAL")
      (finally
        (when @conn (d/release @conn))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-secondary-indexes-fail-clearly
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (assoc (remote-wal-config local-id remote-id)
                   :schema-flexibility :write)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/ident :person/name
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}])
      (try
        (d/transact @conn [{:db/ident :idx/name
                            :db.secondary/type :remote-wal-test/secondary
                            :db.secondary/attrs [:person/name]}])
        (is false "expected remote WAL secondary-index guard to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/unsupported-secondary-index
                 (ex-type e)))))
      (is (nil? (get-in (d/db @conn) [:secondary-indices :idx/name]))
          "failed secondary-index transaction must not publish locally")
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (is (= 1 (count (:datahike/pending (k/get @remote-store :db nil {:sync? true}))))
          "failed secondary-index transaction must not append to the remote WAL")
      (finally
        (when @conn (d/release @conn))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-local-materialization-uses-wal-commit-id
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [tx-report (d/transact @conn [{:db/id 1 :name "Bob"}])
            wal-cid (get-in tx-report [:tx-meta :db/commitId])
            _ (w/materialize-db-with-cid! (:db-after tx-report) wal-cid {:sync? true})
            local-store (ks/connect-store (:store cfg) {:sync? true})]
        (try
          (let [local-head (k/get local-store :db nil {:sync? true})]
            (is (= wal-cid (get-in local-head [:meta :datahike/commit-id]))))
          (finally
            (ks/release-store (:store cfg) local-store {:sync? true}))))
      (finally
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-restart-uses-local-materialized-head-without-replay
  (let [remote-id (uuid)
        cfg (remote-wal-config-with-store (temp-file-store-config) remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [tx-report (d/transact @conn [{:db/id 1 :name "Cached restart"}])
            wal-cid (get-in tx-report [:tx-meta :db/commitId])]
        (w/materialize-db-with-cid! (:db-after tx-report) wal-cid {:sync? true}))
      (d/release @conn)
      (reset! conn nil)

      (with-redefs [w/replay-wal-entry
                    (fn [& _]
                      (throw (ex-info "local materialized WAL head should not replay pending WAL"
                                      {:type :remote-wal-test/unexpected-replay})))]
        (reset! restarted (d/connect cfg))
        (is (= #{["Cached restart"]}
               (d/q '[:find ?n :where [?e :name ?n]] @@restarted))))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-auto-materialization-is-serialized
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (assoc-in (remote-wal-config local-id remote-id)
                      [:writer :wal-auto-materialize?]
                      true)
        remote-store-config (get-in cfg [:writer :remote-store])
        active (atom 0)
        started (atom 0)
        remote-finished (atom 0)
        max-active (atom 0)
        conn (atom nil)]
    (with-redefs [w/materialize-db-with-cid!
                  (fn [& _]
                    (async/thread
                      (let [n (swap! active inc)]
                        (swap! max-active #(max % n))
                        (swap! started inc)
                        (try
                          (Thread/sleep 150)
                          (finally
                            (swap! active dec))))))
                  w/remote-materialize-wal!
                  (fn [& _]
                    (async/thread
                      (swap! remote-finished inc)
                      nil))]
      (try
        (d/create-database cfg)
        (reset! conn (d/connect cfg))
        (d/transact @conn [{:db/id 1 :name "Auto materialize A"}])
        (is (wait-until #(pos? @active) 2000)
            "first materialization should start")
        (d/transact @conn [{:db/id 2 :name "Auto materialize B"}])
        (is (wait-until #(>= @started 2) 5000)
            "second materialization should eventually run")
        (is (wait-until #(zero? @active) 5000)
            "all materialization jobs should finish")
        (is (wait-until #(>= @remote-finished 2) 5000)
            "all remote materialization hooks should finish")
        (is (= 1 @max-active)
            "auto materialization should run one job at a time per writer")
        (finally
          (when @conn (d/release @conn))
          (delete-store-quietly! (:store cfg))
          (delete-store-quietly! remote-store-config))))))

(deftest remote-wal-connect-auto-materializes-reconstructed-head
  (let [local-id (uuid)
        restart-local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        restart-cfg (assoc-in (remote-wal-config restart-local-id remote-id)
                              [:writer :wal-auto-materialize?]
                              true)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)
        remote-store (atom nil)
        restart-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/id 1 :name "Startup materialize"}])
      (d/release @conn)
      (reset! conn nil)

      (reset! restarted (d/connect restart-cfg))
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (is (wait-until #(empty? (:datahike/pending (k/get @remote-store :db nil {:sync? true})))
                      5000)
          "connect should schedule remote materialization when auto-materialize is enabled")
      (let [wal-head (k/get @remote-store :db nil {:sync? true})]
        (is (= (:datahike/wal-head wal-head)
               (:datahike/materialized-head wal-head))))

      (reset! restart-store (ks/connect-store (:store restart-cfg) {:sync? true}))
      (let [local-head (k/get @restart-store :db nil {:sync? true})
            wal-head (k/get @remote-store :db nil {:sync? true})]
        (is (= (:datahike/wal-head wal-head)
               (get-in local-head [:meta :datahike/commit-id]))
            "connect should also materialize the reconstructed head into the local cache"))
      (finally
        (when @restart-store
          (ks/release-store (:store restart-cfg) @restart-store {:sync? true}))
        (when @remote-store
          (ks/release-store remote-store-config @remote-store {:sync? true}))
        (when @restarted (d/release @restarted))
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-replay-preserves-schema-upsert-retractions-and-history
  (let [local-id (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg (assoc (remote-wal-config local-id remote-id)
                   :schema-flexibility :write)
        restart-cfg (assoc (remote-wal-config restart-local-id remote-id)
                           :schema-flexibility :write)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [schema-tx [{:db/ident :email
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/unique :db.unique/identity}
                       {:db/ident :name
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}]
            _ (d/transact @conn schema-tx)
            add-report (d/transact @conn [{:db/id -1
                                            :email "rob@example"
                                            :name "Alice"}])
            eid (get-in add-report [:tempids -1])
            add-tx (get-in add-report [:db-after :max-tx])
            fixed-instant (java.util.Date. 1234567890)
            upsert-report (d/transact @conn {:tx-data [{:db/id -2
                                                         :email "rob@example"
                                                         :name "Alicia"}]
                                             :tx-meta {:db/txInstant fixed-instant}})
            upsert-tx (get-in upsert-report [:db-after :max-tx])]
        (d/transact @conn [[:db/retract eid :name "Alicia"]])
        (is (= fixed-instant (get-in upsert-report [:tx-meta :db/txInstant])))
        (is (= #{[fixed-instant]}
               (d/q '[:find ?t :in $ ?tx :where [?tx :db/txInstant ?t]]
                    @@conn upsert-tx)))
        (is (= #{["rob@example"]}
               (d/q '[:find ?email :where [?e :email ?email]] @@conn)))
        (is (= #{}
               (d/q '[:find ?name :where [?e :name ?name]] @@conn)))
        (is (= #{["Alice"]}
               (d/q '[:find ?name :where [?e :name ?name]]
                    (d/as-of @@conn add-tx))))

        ;; Reconnect with a different local cache to force reconstruction from
        ;; the remote WAL, then verify concrete replay preserved schema changes,
        ;; upsert identity, retraction, txInstant and temporal indexes.
        (d/release @conn)
        (reset! conn nil)
        (reset! restarted (d/connect restart-cfg))
        (is (= #{[fixed-instant]}
               (d/q '[:find ?t :in $ ?tx :where [?tx :db/txInstant ?t]]
                    @@restarted upsert-tx)))
        (is (= #{["rob@example"]}
               (d/q '[:find ?email :where [?e :email ?email]] @@restarted)))
        (is (= #{}
               (d/q '[:find ?name :where [?e :name ?name]] @@restarted)))
        (is (= #{["Alice"]}
               (d/q '[:find ?name :where [?e :name ?name]]
                    (d/as-of @@restarted add-tx)))))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-replay-preserves-max-eid-for-ref-only-tempids
  (let [local-id (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg (assoc (remote-wal-config local-id remote-id)
                   :schema-flexibility :write)
        restart-cfg (assoc (remote-wal-config restart-local-id remote-id)
                           :schema-flexibility :write)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/ident :friend
                          :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/one}])
      (let [tx-report (d/transact @conn [[:db/add -1 :friend -2]])
            expected-max-eid (get-in tx-report [:db-after :max-eid])]
        ;; The value tempid -2 is allocated and becomes max-eid, but it has no
        ;; datom with that eid in entity position. Replay must preserve the WAL
        ;; summary max-eid instead of deriving it only from datom entity ids.
        (is (= 3 expected-max-eid))
        (d/release @conn)
        (reset! conn nil)
        (reset! restarted (d/connect restart-cfg))
        (is (= expected-max-eid (:max-eid @@restarted)))
        (is (= #{[2 3]}
               (d/q '[:find ?e ?f :where [?e :friend ?f]] @@restarted))))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-remote-materialization-clears-pending
  (let [local-id (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        restart-cfg (remote-wal-config restart-local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/id 1 :name "Carol"}])
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (let [materialized (w/remote-materialize-wal! @remote-store :db (dc/load-config cfg))
            wal-head (k/get @remote-store :db nil {:sync? true})]
        (is (w/remote-wal-record? wal-head))
        (is (empty? (:datahike/pending wal-head)))
        (is (= (:datahike/wal-head wal-head)
               (:datahike/materialized-head wal-head)
               (:datahike/materialized-head materialized)))
        (is (w/stored-db? (:datahike/materialized-db wal-head))))

      ;; A fresh local cache can boot from the remote materialized DB without
      ;; pending WAL replay.
      (d/release @conn)
      (reset! conn nil)
      (reset! restarted (d/connect restart-cfg))
      (is (= #{["Carol"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@restarted)))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-corrupt-materialized-checkpoint-fails-clearly
  (let [local-id (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        restart-cfg (remote-wal-config restart-local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/id 1 :name "Checkpointed"}])
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (w/remote-materialize-wal! @remote-store :db (dc/load-config cfg))
      (let [wal-head (k/get @remote-store :db nil {:sync? true})]
        (k/assoc @remote-store :db (dissoc wal-head :datahike/materialized-db) {:sync? true})
        (try
          (d/connect restart-cfg)
          (is false "expected corrupt remote WAL checkpoint to fail")
          (catch clojure.lang.ExceptionInfo e
            (is (= :remote-wal/invalid-head (:type (ex-data e))))
            (is (= :materialized-db-missing (:reason (ex-data e))))))

        (k/assoc @remote-store :db
                 (-> wal-head
                     (assoc :datahike/materialized-head nil)
                     (update-in [:datahike/materialized-db :meta]
                                dissoc :datahike/commit-id))
                 {:sync? true})
        (try
          (d/connect restart-cfg)
          (is false "expected unanchored remote WAL checkpoint to fail")
          (catch clojure.lang.ExceptionInfo e
            (is (= :remote-wal/invalid-head (:type (ex-data e))))
            (is (= :materialized-head-missing (:reason (ex-data e)))))))
      (finally
        (when @conn (d/release @conn))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-stale-writer-catches-up-after-pending-compaction
  (let [local-id-a (uuid)
        local-id-b (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg-a (remote-wal-config local-id-a remote-id)
        cfg-b (remote-wal-config local-id-b remote-id)
        restart-cfg (remote-wal-config restart-local-id remote-id)
        remote-store-config (get-in cfg-a [:writer :remote-store])
        stale-conn (atom nil)
        fresh-conn (atom nil)
        restarted (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg-a)
      (reset! stale-conn (d/connect cfg-a))
      (reset! fresh-conn (d/connect cfg-b))
      (d/transact @fresh-conn [{:db/id 1 :name "Materialized first"}])
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (w/remote-materialize-wal! @remote-store :db (dc/load-config cfg-a))

      ;; `stale-conn` was still at the pre-WAL empty DB when another writer's
      ;; entry was materialized and removed from pending. Its next transaction
      ;; must reload the remote materialized checkpoint before speculating;
      ;; replaying the remaining pending suffix on the empty DB would produce
      ;; an invalid WAL summary and lose "Materialized first" locally.
      (d/transact @stale-conn [{:db/id 2 :name "After compaction"}])
      (is (= #{["Materialized first"] ["After compaction"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@stale-conn)))

      ;; A second materialization validates that the stale writer's WAL entry
      ;; summary was built from the caught-up DB, not from its old empty state.
      (w/remote-materialize-wal! @remote-store :db (dc/load-config cfg-a))
      (d/release @stale-conn)
      (reset! stale-conn nil)
      (d/release @fresh-conn)
      (reset! fresh-conn nil)
      (reset! restarted (d/connect restart-cfg))
      (is (= #{["Materialized first"] ["After compaction"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@restarted)))
      (finally
        (when @stale-conn (d/release @stale-conn))
        (when @fresh-conn (d/release @fresh-conn))
        (when @restarted (d/release @restarted))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg-a))
        (delete-store-quietly! (:store cfg-b))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-fresh-file-cache-materializes-after-remote-checkpoint
  (let [remote-id (uuid)
        cfg (remote-wal-config-with-store (temp-file-store-config) remote-id)
        restart-cfg (remote-wal-config-with-store (temp-file-store-config) remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)
        reloaded (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/id 1 :name "Disk A"}])
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (w/remote-materialize-wal! @remote-store :db (dc/load-config cfg))
      (d/release @conn)
      (reset! conn nil)

      ;; The restarted writer has an empty file cache and must localize the
      ;; remote checkpoint before future local materialization flushes indexes.
      (reset! restarted (d/connect restart-cfg))
      (let [tx-report (d/transact @restarted [{:db/id 2 :name "Disk B"}])
            wal-cid (get-in tx-report [:tx-meta :db/commitId])]
        (w/materialize-db-with-cid! (:db-after tx-report) wal-cid {:sync? true}))
      (d/release @restarted)
      (reset! restarted nil)

      (reset! reloaded (d/connect restart-cfg))
      (is (= #{["Disk A"] ["Disk B"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@reloaded)))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (when @reloaded (d/release @reloaded))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-cas-conflict-retries-original-transaction
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [real-cas w/cas-assoc!
            cas-attempts (atom 0)
            tx-report (with-redefs [w/cas-assoc! (fn [& args]
                                                    (if (= 1 (swap! cas-attempts inc))
                                                      (let [ch (async/promise-chan)]
                                                        (async/put! ch :conflict)
                                                        ch)
                                                      (apply real-cas args)))]
                        (d/transact @conn [{:db/id 1 :name "Retried"}]))
            remote-store (ks/connect-store remote-store-config {:sync? true})
            wal-head (k/get remote-store :db nil {:sync? true})]
        (is (map? tx-report))
        (is (= 2 @cas-attempts))
        (is (= #{["Retried"]}
               (d/q '[:find ?n :where [?e :name ?n]] @@conn)))
        (is (= 1 (count (:datahike/pending wal-head))))
        (is (= (:datahike/wal-head wal-head)
               (get-in tx-report [:tx-meta :db/commitId])))
        (ks/release-store remote-store-config remote-store {:sync? true}))
      (finally
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-cas-conflict-preserves-existing-pending-index-writes
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [pending-writes (-> @@conn :store :storage :pending-writes)
            sentinel [(uuid) {:remote-wal-test/sentinel true}]
            _ (swap! pending-writes conj sentinel)
            real-cas w/cas-assoc!
            cas-attempts (atom 0)
            tx-report (with-redefs [w/cas-assoc! (fn [& args]
                                                    (if (= 1 (swap! cas-attempts inc))
                                                      (let [ch (async/promise-chan)]
                                                        (async/put! ch :conflict)
                                                        ch)
                                                      (apply real-cas args)))]
                        (d/transact @conn [{:db/id 1 :name "Pending survives"}]))]
        (is (map? tx-report))
        (is (= 2 @cas-attempts))
        (is (some #{sentinel} @pending-writes)
            "CAS retry must not clear store-level pending writes that may belong to background materialization"))
      (finally
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-cas-conflict-exhaustion-discards-speculative-db
  (let [local-id (uuid)
        remote-id (uuid)
        cfg (assoc-in (remote-wal-config local-id remote-id)
                      [:writer :wal-max-retries]
                      0)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [cas-attempts (atom 0)]
        (try
          (with-redefs [w/cas-assoc! (fn [& _args]
                                        (swap! cas-attempts inc)
                                        (let [ch (async/promise-chan)]
                                          (async/put! ch :conflict)
                                          ch))]
            (d/transact @conn [{:db/id 1 :name "Speculative loser"}]))
          (is false "expected remote WAL CAS exhaustion to fail")
          (catch clojure.lang.ExceptionInfo e
            (is (= :remote-wal/cas-retries-exhausted (ex-type e))))
          (finally
            (is (= 1 @cas-attempts)
                "wal-max-retries 0 should make exactly one CAS attempt"))))
      (is (empty? (d/q '[:find ?n :where [?e :name ?n]] @@conn))
          "failed CAS attempts must not publish speculative state locally")
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (let [wal-head (k/get @remote-store :db nil {:sync? true})]
        (is (nil? (:datahike/wal-head wal-head)))
        (is (empty? (:datahike/pending wal-head))
            "failed CAS attempts must not append to the remote WAL"))
      (finally
        (when @conn (d/release @conn))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-two-writers-produce-one-wal-order
  (let [local-id-a (uuid)
        local-id-b (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg-a (remote-wal-config local-id-a remote-id)
        cfg-b (remote-wal-config local-id-b remote-id)
        restart-cfg (remote-wal-config restart-local-id remote-id)
        remote-store-config (get-in cfg-a [:writer :remote-store])
        conn-a (atom nil)
        conn-b (atom nil)
        restarted (atom nil)]
    (try
      (d/create-database cfg-a)
      (reset! conn-a (d/connect cfg-a))
      (reset! conn-b (d/connect cfg-b))
      (let [fa (future (d/transact @conn-a [{:db/id 1 :name "Ada"}]))
            fb (future (d/transact @conn-b [{:db/id 2 :name "Bea"}]))
            reports [@fa @fb]
            remote-store (ks/connect-store remote-store-config {:sync? true})
            wal-head (k/get remote-store :db nil {:sync? true})
            pending (vec (:datahike/pending wal-head))]
        (is (every? map? reports))
        (is (= 2 (count pending)))
        (is (= (:datahike/wal-head wal-head)
               (:datahike/wal-id (peek pending))))
        (is (= (:datahike/wal-parent (second pending))
               (:datahike/wal-id (first pending))))
        (ks/release-store remote-store-config remote-store {:sync? true}))

      (reset! restarted (d/connect restart-cfg))
      (is (= #{["Ada"] ["Bea"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@restarted)))
      (finally
        (when @conn-a (d/release @conn-a))
        (when @conn-b (d/release @conn-b))
        (when @restarted (d/release @restarted))
        (delete-store-quietly! (:store cfg-a))
        (delete-store-quietly! (:store cfg-b))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-cas-loser-revalidates-unique-conflict-after-retry
  (let [local-id-a (uuid)
        local-id-b (uuid)
        remote-id (uuid)
        cfg-a (assoc (remote-wal-config local-id-a remote-id)
                     :schema-flexibility :write)
        restart-cfg (assoc (remote-wal-config (uuid) remote-id)
                           :schema-flexibility :write)
        cfg-b (assoc (remote-wal-config local-id-b remote-id)
                     :schema-flexibility :write)
        remote-store-config (get-in cfg-a [:writer :remote-store])
        conn-a (atom nil)
        conn-b (atom nil)
        restarted (atom nil)]
    (try
      (d/create-database cfg-a)
      (reset! conn-a (d/connect cfg-a))
      (reset! conn-b (d/connect cfg-b))
      (d/transact @conn-a [{:db/ident :email
                            :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/unique :db.unique/value}])
      (let [real-cas w/cas-assoc!
            first-data-cas-latch (java.util.concurrent.CountDownLatch. 2)
            data-cas-attempts (atom 0)
            transact-dup (fn [conn eid]
                           (future
                             (try
                               (d/transact conn [{:db/id eid :email "dupe@example"}])
                               (catch Throwable e
                                 e))))
            [result-a result-b]
            (with-redefs [w/cas-assoc!
                          (fn [store key expected-etag new-value opts]
                            (when (= 2 (count (:datahike/pending new-value)))
                              (swap! data-cas-attempts inc)
                              (.countDown first-data-cas-latch)
                              (is (.await first-data-cas-latch 5 java.util.concurrent.TimeUnit/SECONDS)))
                            (real-cas store key expected-etag new-value opts))]
              (let [fa (transact-dup @conn-a 100)
                    fb (transact-dup @conn-b 200)]
                [@fa @fb]))
            reports (filter map? [result-a result-b])
            errors (remove map? [result-a result-b])
            error (first errors)
            error-message (if (instance? Throwable error)
                            (ex-message error)
                            (str error))
            remote-store (ks/connect-store remote-store-config {:sync? true})
            wal-head (k/get remote-store :db nil {:sync? true})]
        (is (= 2 @data-cas-attempts))
        (is (= 1 (count reports)))
        (is (= 1 (count errors)))
        (is (re-find #"unique constraint" (or error-message "")))
        (is (= 2 (count (:datahike/pending wal-head)))
            "only the schema tx and the winning data tx should be globally committed")
        (ks/release-store remote-store-config remote-store {:sync? true})
        (reset! restarted (d/connect restart-cfg))
        (is (= #{["dupe@example"]}
               (d/q '[:find ?email :where [?e :email ?email]] @@restarted))))
      (finally
        (when @conn-a (d/release @conn-a))
        (when @conn-b (d/release @conn-b))
        (when @restarted (d/release @restarted))
        (delete-store-quietly! (:store cfg-a))
        (delete-store-quietly! (:store cfg-b))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-replay-ignores-local-version-metadata
  (let [local-id (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        restart-cfg (remote-wal-config restart-local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)
        remote-store (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn [{:db/id 1 :name "Version tolerant"}])
      (reset! remote-store (ks/connect-store remote-store-config {:sync? true}))
      (let [wal-head (k/get @remote-store :db nil {:sync? true})
            drifted-head (update wal-head :datahike/pending
                                  (fn [pending]
                                    (mapv #(update-in % [:datahike/db-after :meta]
                                                      merge
                                                      {:datahike/version "future-datahike"
                                                       :hitchhiker.tree/version "future-hht"
                                                       :persistent.set/version "future-pss"
                                                       :konserve/version "future-konserve"})
                                          pending)))]
        (k/assoc @remote-store :db drifted-head {:sync? true}))

      ;; Replaying a WAL written by another Datahike/konserve version should
      ;; validate logical DB state, not local library metadata in DB meta.
      (d/release @conn)
      (reset! conn nil)
      (reset! restarted (d/connect restart-cfg))
      (is (= #{["Version tolerant"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@restarted)))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (when @remote-store (ks/release-store remote-store-config @remote-store {:sync? true}))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))

(deftest remote-wal-single-writer-replays-from-remote
  (let [local-id (uuid)
        remote-id (uuid)
        restart-local-id (uuid)
        cfg (remote-wal-config local-id remote-id)
        restart-cfg (remote-wal-config restart-local-id remote-id)
        remote-store-config (get-in cfg [:writer :remote-store])
        conn (atom nil)
        restarted (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (let [tx-report (d/transact @conn [{:db/id 1 :name "Alice"}])
            remote-store (ks/connect-store remote-store-config {:sync? true})
            wal-head (k/get remote-store :db nil {:sync? true})
            wal-entry (peek (:datahike/pending wal-head))]
        (is (= #{["Alice"]}
               (d/q '[:find ?n :where [?e :name ?n]] @@conn)))
        (is (w/remote-wal-record? wal-head))
        (is (= 1 (count (:datahike/pending wal-head))))
        (is (= (:datahike/wal-head wal-head)
               (get-in tx-report [:tx-meta :db/commitId])))
        (is (= (:datahike/wal-head wal-head)
               (get-in @@conn [:meta :datahike/commit-id])))
        (is (nil? (get-in wal-entry [:datahike/db-after :meta :datahike/commit-id]))
            "WAL db-after summaries must not carry a stale parent commit id")
        (is (= (get-in tx-report [:tx-meta :db/txInstant])
               (get-in wal-entry [:datahike/db-after :meta :datahike/updated-at]))))

      ;; Use a different local cache id to prove the committed state comes from
      ;; the remote WAL, not from the first process' local snapshot.
      (d/release @conn)
      (reset! conn nil)
      (reset! restarted (d/connect restart-cfg))
      (is (= #{["Alice"]}
             (d/q '[:find ?n :where [?e :name ?n]] @@restarted)))
      (finally
        (when @conn (d/release @conn))
        (when @restarted (d/release @restarted))
        (delete-store-quietly! (:store cfg))
        (delete-store-quietly! (:store restart-cfg))
        (delete-store-quietly! remote-store-config)))))
