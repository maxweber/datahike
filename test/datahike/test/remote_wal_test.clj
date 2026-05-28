(ns datahike.test.remote-wal-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.config :as dc]
            [datahike.writing :as w]
            [konserve.core :as k]
            [konserve.store :as ks]))

(defn- uuid []
  (java.util.UUID/randomUUID))

(defn- delete-store-quietly! [store-config]
  (try
    (ks/delete-store store-config {:sync? true})
    (catch Throwable _)))

(defn- remote-wal-config [local-id remote-id]
  {:store {:backend :memory :id local-id}
   :schema-flexibility :read
   :keep-history? true
   :writer {:backend :remote-wal
            :remote-store {:backend :memory :id remote-id}
            :wal-branch :db
            :wal-auto-materialize? false
            :wal-max-retries 3}})

(deftest remote-wal-config-validation
  (testing "remote WAL requires an explicit durable remote store"
    (let [cfg {:store {:backend :memory :id (uuid)}
               :writer {:backend :remote-wal}}]
      (try
        (dc/load-config cfg)
        (is false "expected remote WAL config validation to fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :remote-wal/missing-remote-store (:type (ex-data e))))))))

  (testing "stores without explicit CAS support fail clearly"
    (try
      (w/cas-assoc! {} :db nil {})
      (is false "expected missing CAS helper to fail")
      (catch clojure.lang.ExceptionInfo e
        (is (= :remote-wal/cas-unavailable (:type (ex-data e))))))))

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
            local-store (ks/connect-store (:store cfg) {:sync? true})
            local-head (k/get local-store :db nil {:sync? true})]
        (is (= wal-cid (get-in local-head [:meta :datahike/commit-id]))))
      (finally
        (when @conn (d/release @conn))
        (delete-store-quietly! (:store cfg))
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
            wal-head (k/get remote-store :db nil {:sync? true})]
        (is (= #{["Alice"]}
               (d/q '[:find ?n :where [?e :name ?n]] @@conn)))
        (is (w/remote-wal-record? wal-head))
        (is (= 1 (count (:datahike/pending wal-head))))
        (is (= (:datahike/wal-head wal-head)
               (get-in tx-report [:tx-meta :db/commitId])))
        (is (= (:datahike/wal-head wal-head)
               (get-in @@conn [:meta :datahike/commit-id]))))

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
