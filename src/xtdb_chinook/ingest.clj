(ns xtdb-chinook.ingest
  "Translates that data from sqlite to xt via next.jdbc."
  (:require [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [camel-snake-kebab.core :as csk]))

(def con (jdbc/get-connection
          (jdbc/get-datasource {:dbtype "sqlite"
                                :dbname "resources/chinook.db"})))

(defn remap-ns-key [k old new]
  (if (= (namespace k) old)
    (keyword new (name k))
    k))

(defn kw-camelcase->kw-kebab-case [kw]
  (if-let [ns (namespace kw)]
    (keyword (csk/->kebab-case ns)
             (csk/->kebab-case (name kw)))
    (csk/->kebab-case kw)))

(comment
  (kw-camelcase->kw-kebab-case :foo_id/FooBar)
  (kw-camelcase->kw-kebab-case :FooBar))

(def tracks (->> (jdbc/execute! con ["SELECT * FROM tracks;"])
                 (map #(update-keys % (fn [k] (remap-ns-key k "tracks" "track"))))
                 (map (fn [{:track/keys [TrackId GenreId MediaTypeId AlbumId] :as track}]
                        (assoc track
                               :xt/id (keyword (name 'track) (str "id-" TrackId))
                               :track/genre (keyword (name 'genre) (str "id-" GenreId))
                               :track/media-type (keyword (name 'media-type) (str "id-" MediaTypeId))
                               :track/album (keyword (name 'album) (str "id-" AlbumId)))))
                 (map #(update-keys % kw-camelcase->kw-kebab-case))
                 (map #(dissoc % :track/genre-id :track/media-type-id :track/album-id))))

(def media-types (->> (jdbc/execute! con ["SELECT * FROM media_types;"])
                      (map #(update-keys % (fn [k] (remap-ns-key k "media_types" "media-type"))))
                      (map (fn [{:media-type/keys [MediaTypeId] :as media-type}]
                             (assoc media-type :xt/id (keyword (name 'media-type) (str "id-" MediaTypeId)))))
                      (map #(update-keys % kw-camelcase->kw-kebab-case))))

(def genres (->> (jdbc/execute! con ["SELECT * FROM genres;"])
                 (map #(update-keys % (fn [k] (remap-ns-key k "genres" "genre"))))
                 (map (fn [{:genre/keys [GenreId] :as genre}]
                        (assoc genre :xt/id (keyword (name 'genre) (str "id-" GenreId)))))
                 (map #(update-keys % kw-camelcase->kw-kebab-case))))

(def playlist-tracks (->> (jdbc/execute! con ["SELECT * FROM playlist_track;"])
                          (map #(update-keys % (fn [k] (remap-ns-key k "playlist_track" "playlist-track"))))
                          (map (fn [{:playlist-track/keys [PlaylistId TrackId] :as playlist-track}]
                                 (assoc playlist-track
                                        :xt/id (keyword (name 'playlist-track) (str "id-" PlaylistId "-" TrackId))
                                        :playlist-track/playlist (keyword (name 'playlist) (str "id-" PlaylistId))
                                        :playlist-track/track (keyword (name 'track) (str "id-" TrackId)))))
                          (map #(update-keys % kw-camelcase->kw-kebab-case))
                          (map #(dissoc % :playlist-track/playlist-id :playlist-track/track-id))))

(def playlists (->> (jdbc/execute! con ["SELECT * FROM playlists;"])
                    (map #(update-keys % (fn [k] (remap-ns-key k "playlists" "playlist"))))
                    (map (fn [{:playlist/keys [PlaylistId ] :as playlist-track}]
                           (assoc playlist-track
                                  :xt/id (keyword (name 'playlist) (str "id-" PlaylistId)))))
                    (map #(update-keys % kw-camelcase->kw-kebab-case))
                    #_(map #(dissoc % :playlist-track/playlist-id :playlist-track/track-id))))

(def albums (->> (jdbc/execute! con ["SELECT * FROM albums;"])
                 (map #(update-keys % (fn [k] (remap-ns-key k "albums" "album"))))
                 (map (fn [{:album/keys [AlbumId ArtistId] :as album}]
                        (assoc album
                               :xt/id (keyword (name 'album) (str "id-" AlbumId))
                               :album/artist (keyword (name 'artist) (str "id-" ArtistId)))))
                 (map #(update-keys % kw-camelcase->kw-kebab-case))
                 (map #(dissoc % :album/artist-id))))

(def artists (->> (jdbc/execute! con ["SELECT * FROM artists;"])
                  (map #(update-keys % (fn [k] (remap-ns-key k "artists" "artist"))))
                  (map (fn [{:artist/keys [ArtistId] :as artist}]
                         (assoc artist :xt/id (keyword (name 'artist) (str "id-" ArtistId)))))
                  (map #(update-keys % kw-camelcase->kw-kebab-case))))

(def transactions (->> (mapcat identity [tracks media-types genres playlist-tracks
                                         playlists albums artists])
                       (map #(vector ::xt/put %))))

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

(defn db [] (xt/db xtdb-node))

(xt/submit-tx xtdb-node transactions)

(xt/q (db)
      '{:find [?name]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)" ]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?name]]})
