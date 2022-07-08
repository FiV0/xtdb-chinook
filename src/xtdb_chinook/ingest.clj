(ns xtdb-chinook.ingest
  "Ingests that data from the sqlite db into an xt instance."
  (:require [clojure.java.io :as io]
            [clojure.set]
            [xtdb-chinook.export :as export]
            [xtdb.api :as xt]))

;; xt
(defn start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store "data/tx-log")
      :xtdb/document-store (kv-store "data/doc-store")
      :xtdb/index-store (kv-store "data/index-store")})))

(def xtdb-node (start-xtdb!))

(defn stop-xtdb! [] (.close xtdb-node))

(defn ingest [_]
  (xt/submit-tx xtdb-node
                (->> (export/chinook-entities export/con)
                     (map #(clojure.set/rename-keys % {:db/id :xt/id}))
                     (map #(vector ::xt/put %))
                     doall))
  (xt/sync xtdb-node)
  (stop-xtdb!))

(comment
  (ingest nil)

  (->> (read-string (slurp "entities.edn"))
       (map #(clojure.set/rename-keys % {:db/id :xt/id})))

  )
