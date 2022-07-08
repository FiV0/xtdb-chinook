(ns xtdb-chinook.export
  "Exports and translates the data from the sqlite db to edn writable maps."
  (:require [next.jdbc :as jdbc]
            [camel-snake-kebab.core :as csk]))

(defonce con (jdbc/get-connection (jdbc/get-datasource {:dbtype "sqlite"
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

(defn tracks [con]
  (->> (jdbc/execute! con ["SELECT * FROM tracks;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "tracks" "track"))))
       (map (fn [{:track/keys [TrackId GenreId MediaTypeId AlbumId] :as track}]
              (assoc track
                     :db/id (keyword (name 'track) (str "id-" TrackId))
                     :track/genre (keyword (name 'genre) (str "id-" GenreId))
                     :track/media-type (keyword (name 'media-type) (str "id-" MediaTypeId))
                     :track/album (keyword (name 'album) (str "id-" AlbumId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))
       (map #(dissoc % :track/genre-id :track/media-type-id :track/album-id))))

(defn media-types [con]
  (->> (jdbc/execute! con ["SELECT * FROM media_types;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "media_types" "media-type"))))
       (map (fn [{:media-type/keys [MediaTypeId] :as media-type}]
              (assoc media-type :db/id (keyword (name 'media-type) (str "id-" MediaTypeId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))))

(defn genres [con]
  (->> (jdbc/execute! con ["SELECT * FROM genres;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "genres" "genre"))))
       (map (fn [{:genre/keys [GenreId] :as genre}]
              (assoc genre :db/id (keyword (name 'genre) (str "id-" GenreId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))))

(defn playlist-tracks [con]
  (->> (jdbc/execute! con ["SELECT * FROM playlist_track;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "playlist_track" "playlist-track"))))
       (map (fn [{:playlist-track/keys [PlaylistId TrackId] :as playlist-track}]
              (assoc playlist-track
                     :db/id (keyword (name 'playlist-track) (str "id-" PlaylistId "-" TrackId))
                     :playlist-track/playlist (keyword (name 'playlist) (str "id-" PlaylistId))
                     :playlist-track/track (keyword (name 'track) (str "id-" TrackId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))
       (map #(dissoc % :playlist-track/playlist-id :playlist-track/track-id))))

(defn playlists [con]
  (->> (jdbc/execute! con ["SELECT * FROM playlists;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "playlists" "playlist"))))
       (map (fn [{:playlist/keys [PlaylistId] :as playlist-track}]
              (assoc playlist-track
                     :db/id (keyword (name 'playlist) (str "id-" PlaylistId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))
       #_(map #(dissoc % :playlist/playlist-id))))

(defn albums [con]
  (->> (jdbc/execute! con ["SELECT * FROM albums;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "albums" "album"))))
       (map (fn [{:album/keys [AlbumId ArtistId] :as album}]
              (assoc album
                     :db/id (keyword (name 'album) (str "id-" AlbumId))
                     :album/artist (keyword (name 'artist) (str "id-" ArtistId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))
       (map #(dissoc % :album/artist-id))))

(defn artists [con]
  (->> (jdbc/execute! con ["SELECT * FROM artists;"])
       (map #(update-keys % (fn [k] (remap-ns-key k "artists" "artist"))))
       (map (fn [{:artist/keys [ArtistId] :as artist}]
              (assoc artist :db/id (keyword (name 'artist) (str "id-" ArtistId)))))
       (map #(update-keys % kw-camelcase->kw-kebab-case))))

(defn chinook-entities [con]
  (mapcat #(% con) [tracks media-types genres playlist-tracks playlists albums artists]))

(defn write-entities [{:keys [path] :or {path "resources/transactions.edn"}}]
  (spit path (seq (chinook-entities con))))

(comment
  (write-entities {:path "entities.edn"}))
