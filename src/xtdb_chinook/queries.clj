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

;; custom aggregates need to be specified via clojures `defmethod`

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

;; In the pull syntax you also have the option to do reverse lookup.
;; In the following we query for the name and all the tracks refering
;; to the first album.
(xt/pull-many (db) '[:album/name {:track/_album [*]}] [:album/id-1])

;; Returning maps
;; With the `:keys` keyword you can specify the keys for maps to return.

(xt/q (db)
      '{:find [?album-name ?artist-name]
        :keys [album artist]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)"]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?album :album/title ?album-name]
         [?artist :artist/name ?artist-name]]})

;; Other options are `:syms` and `:strs`

;; When restraining the results in the where clauses you also have the
;; possibility to restrain the results via predicates. Any fully
;; qualified Clojure function that returns a boolean is acceptable.

;; Getting all the tracks which are at least an hour long
(xt/q (db)
      '{:find [(pull ?track [*])]
        :where
        [[?track :track/milliseconds millis]
         [(<= 3600000 millis)]]})

;; TODO subquery

;; Unification predicates
;; What is the difference?
(xt/q (db)
      '{:find [?track1 ?track2]
        :where
        [[?track1 :track/unit-price uprice1]
         [?track2 :track/unit-price uprice2]
         [(not= uprice1 uprice2)]]})

(xt/q (db)
      '{:find [?track1 ?track2]
        :where
        [[?track1 :track/unit-price uprice1]
         [?track2 :track/unit-price uprice2]
         [(!= uprice1 uprice2)]]})

;; `not` rejects all tuples for which the clauses inside the not are true.
;; Getting all track which are not Rock
(xt/q (db)
      '{:find [(pull ?track [*])]
        :where
        [[?track :xt/id]
         [?track :track/genre ?genre]
         (not [?genre :genre/name "Rock"])]})

;; In `not-join` you need to first specify the logic variables to unify with the outer query.
(xt/q (db)
      '{:find [(pull ?track [*])]
        :where
        [[?track :xt/id]
         (not-join [?track]
                   [?track :track/genre ?genre]
                   [?genre :genre/name "Rock"])]})

;; `or` is satisfied if one of it's clauses is satisfied
;; Querying for tracks that are either Rock or Rock and Roll
(xt/q (db)
      '{:find [(pull ?track [*])]
        :where
        [[?track :xt/id]
         [?track :track/genre ?genre]
         (or [?genre :genre/name "Rock"]
             [?genre :genre/name "Rock And Roll"])]})

;; `or-join` is to `or` what is`not-join` to `not`
(xt/q (db)
      '{:find [(pull ?track [*])]
        :where
        [[?track :xt/id]
         (or-join [?track]
                  [?track :track/genre ?genre]
                  [?genre :genre/name "Rock"]
                  [?genre :genre/name "Rock And Roll"])]})
