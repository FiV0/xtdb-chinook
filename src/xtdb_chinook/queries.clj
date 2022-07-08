(ns xtdb-chinook.queries
  "A namespace to demonstrate the query capabilities of xtdb."
  (:require [xtdb-chinook.ingest :as ingest]
            [xtdb.api :as xt]
            [xtdb.query]))

(defn db [] (xt/db ingest/xtdb-node))

;; The following query find's the name of the artist who played the
;; track "For Those About To Rock (We Salute You)" via
;; ?track -> ?album -> ?artist

(xt/q (db)
      '{:find [?name]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)"]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?name]]})

;; One can use clojure.core functions inside the where clause

(xt/q (db)
      '{:find [(str ?name " is awesome!")]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)"]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?name]]})

;; The notion of group-by is implicit. For example if you want to count
;; the number of tracks by AC/CD

(xt/q (db)
      '{:find [(count ?t)]
        :where
        [[?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name "AC/DC"]]})

;; custom aggregates need to specified via clojures `defmethod`

(defmethod xtdb.query/aggregate 'sort-reverse [_]
  (fn
    ([] [])
    ([acc] (vec (reverse (sort acc))))
    ([acc x] (conj acc x))))

(xt/q (db)
      '{:find [?artist-name (sort-reverse ?track-name)]
        :where
        [[?t :track/name ?track-name]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?artist-name]]})

;; Pull let's you specify what projection (selection) you want on your entities.

(xt/q (db)
      '{:find [(pull ?track [:track/name :track/composer])]
        :where [[?track :track/track-id]]})

;; There is also a wildcard option

(xt/q (db)
      '{:find [(pull ?track [*])]
        :where [[?track :track/track-id]]
        :limit 10})

;; One can also pull and pull-many directly

(xt/pull (db) '[*] :track/id-1)
(xt/pull-many (db) '[*] [:track/id-1 :track/id-2])
