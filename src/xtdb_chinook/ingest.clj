(ns xtdb-chinook.ingest
  "Ingests that data from the sqlite db into an xt instance."
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.set]
            [xtdb-chinook.export :as export]
            [xtdb.client :as client]
            [xtdb.api :as xt]))


(def xtdb-node (client/start-client "http://localhost:3000"))

(defn stop-xtdb! [] (.close xtdb-node))

(defn normalize-entity [e]
  (-> (dissoc e :xt/id)
      (update-keys #(keyword (name %)))
      (assoc :xt/id (:xt/id e))))

(defn read-entities []
  (->> (edn/read-string (slurp "entities.edn"))
       (map normalize-entity)))

(defn entity->table [entity]
  (-> entity :xt/id namespace keyword))

(comment
  (entity->table (first entities)))

(defn ingest [entities]
  (->> (map #(vector :put (entity->table %) %) entities)
       (partition-all 1024)
       (mapv #(xt/submit-tx xtdb-node (vec %)))))

(comment
  (def entities (read-entities))
  (first entities)

  (ingest entities)

  (xt/q xtdb-node '(from :track [name composer]))

  )
