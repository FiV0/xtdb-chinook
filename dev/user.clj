(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as repl]
            [lambdaisland.classpath.watch-deps :as watch-deps]
            [xtdb.api :as xt]
            [xtdb.client :as xt.client]
            ))

(defn watch-deps!
  []
  (watch-deps/start! {:aliases [:dev :test]}))

(defn go []
  (watch-deps!))

(comment

  (def my-node (xt.client/start-client "http://localhost:3000"))

  (xt/status my-node)




  )
