(ns ^{:clojure.tools.namespace.repl/load false
      :clojure.tools.namespace.repl/unload false} user
  (:require
   ;; Defaults copied from clojure.main
   [clojure.repl :refer (source apropos dir pst doc find-doc)]
   [clojure.java.javadoc :refer (javadoc)]
   [clojure.pprint :refer (pp pprint)]
   ;; very common convenience ns aliases
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :as ctn]))

(ctn/set-refresh-dirs "src" "test")

(defn reset []
  (ctn/refresh))
