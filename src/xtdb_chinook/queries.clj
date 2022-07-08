(ns xtdb-chinook.queries
  "A namespace to demonstrate the query capabilities of xtdb."
  (:require [xtdb-chinook.ingest :as ingest]
            [xtdb.api :as xt]))

(defn db [] (xt/db ingest/xtdb-node))

(xt/q (db)
      '{:find [?name]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)" ]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?name]]})
