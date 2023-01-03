(ns clj-pgcopy.benchmark
  (:require [criterium.core :as crit]
            [clj-pgcopy.core :as copy]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            ;; [clojure.test :refer [join-fixtures]]
            [clojure.string :as str]
            [crockery.core :as crock])
  (:import (java.util TimeZone Random)))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            (map keyword)    ;; Drop if you want string keys instead
            repeat)
       (rest csv-data)))

(defn read-csv [path]
  (with-open [rdr (io/reader (io/resource path))]
    (into []
          (csv-data->maps (csv/read-csv rdr)))))

(defn load-fight-songs []
  (let [data (read-csv "fight-songs.csv")
        ->bool (fn [v] (= "yes" (str/lower-case v)))
        ->int (fn [v] (try (Integer/parseInt v) (catch Exception e)))]
    (mapv (fn [m]
            (-> m
                (update :rah ->bool)
                (update :opponents ->bool)
                (update :official_song ->bool)
                (update :contest ->bool)
                (update :victory ->bool)
                (update :colors ->bool)
                (update :win_won ->bool)
                (update :student_writer ->bool)
                (update :fight ->bool)
                (update :victory_win_won ->bool)
                (update :nonsense ->bool)
                (update :spelling ->bool)
                (update :men ->bool)
                (update :trope_count ->int)
                (update :number_fights ->int)
                (update :year ->int)
                (update :bpm ->int)
                (update :sec_duration ->int)
                )) data)))

(extend-protocol jdbc/ISQLValue
  java.time.Instant
  (sql-value [v]
    (java.sql.Timestamp/from v))
  java.time.LocalDate
  (sql-value [v]
    (java.sql.Date/valueOf v)))

(def conn-spec "jdbc:postgresql://localhost:5432/test_pgcopy")

;; fixtures
(defmacro with-bench-tables [& body]
  `(jdbc/with-db-connection [conn# conn-spec]
     (jdbc/execute! conn# "drop schema if exists benchmark cascade")
     (jdbc/execute! conn# "create schema benchmark")
     (jdbc/execute! conn# (str "create table benchmark.typical("
                               "guid uuid primary key,"
                               "created_at timestamptz not null,"
                               "active boolean not null default false,"
                               "price decimal(8,2),"
                               "average_rating float4"
                               ")"))
     (jdbc/execute! conn# (str "create table benchmark.bindata("
                               "guid uuid primary key,"
                               "payload bytea not null,"
                               "additonal bytea"
                               ")"))
     (jdbc/execute! conn# (str "create table benchmark.textual("
                               "guid uuid primary key,"
                               "title varchar not null,"
                               "body text not null"
                               ")"))
     (jdbc/execute! conn# (str "create table benchmark.fight_songs("
                               "school text primary key,"
                               "conference varchar,"
                               "song_name varchar,"
                               "writers text,"
                               "year int,"
                               "student_writer boolean,"
                               "official_song boolean,"
                               "bpm int,"
                               "sec_duration int,"
                               "trope_count int,"
                               "fight boolean,"
                               "num_fights int,"
                               "spotify_id text,"
                               "victory boolean,"
                               "win_won boolean,"
                               "victory_win_won boolean,"
                               "rah boolean,"
                               "nonsense boolean,"
                               "colors boolean,"
                               "men boolean,"
                               "opponents boolean,"
                               "spelling boolean"
                               ")"))

     (try
       ~@body
       (finally
         (jdbc/execute! conn# "drop schema if exists benchmark cascade")))))

(defmacro with-utc [& body]
  `(let [original# (TimeZone/getDefault)]
     (TimeZone/setDefault (TimeZone/getTimeZone "Z"))
     (try
       ~@body
       (finally
         (TimeZone/setDefault original#)))))

(def available-characters
  (map char
       (range 64 123)))

(defn gen-random-string [len]
  (let [sb (StringBuilder.)]
    (doseq [_ (range len)]
      (.append sb
               (rand-nth available-characters)))
    (str sb)))

(defn gen-sentence []
  (str/join " "
            (map (fn [_]
                   (gen-random-string (+ 3 (rand-int 10))))
                 (range (+ 5 (rand-int 50))))))

(defn generate-inventory [total-rows]
  (let [now (java.time.Instant/now)]
    (into []
          (map (fn [_]
                 {:guid (java.util.UUID/randomUUID)
                  :created_at (.minusMillis now (rand-int 100000000))
                  :active (rand-nth [true false])
                  :price (.setScale (bigdec (+ 10 (rand 1000))) 2 java.math.RoundingMode/HALF_UP)
                  :average_rating (-> (rand-int 50) float (/ 10) float)}))
          (range total-rows))))

(defn generate-bindata [total-rows]
  (into []
        (map (fn [_]
               {:guid (java.util.UUID/randomUUID)
                :payload (let [b (byte-array (+ 128 (rand-int 9096)))
                               r (Random.)]
                           (.nextBytes r b)
                           b)
                :additional (when (rand-nth [true false])
                              (let [b (byte-array (+ 10 (rand-int 480)))
                                    r (Random.)]
                                (.nextBytes r b)
                                b))}))
        (range total-rows)))

(defn generate-textual [total-rows]
  (into []
        (map (fn [_]
               {:guid (java.util.UUID/randomUUID)
                :title (gen-random-string (+ 5 (rand-int 20)))
                :body (str/join "\n"
                                (map (fn [_] (gen-sentence))
                                     (range (+ 4 (rand-int 5)))))}))
        (range total-rows)))

(defn bench-insert-multi [table cols batches]
  (let [->tuple (apply juxt cols)]
    (jdbc/with-db-connection [conn conn-spec]
      (jdbc/execute! conn (str "truncate " (name table)))
      (doseq [batch batches]
        (jdbc/insert-multi! conn table cols (map ->tuple batch))))))

(defn bench-pgcopy [table cols batches]
  (let [->tuple (apply juxt cols)]
    (jdbc/with-db-connection [conn conn-spec]
      (jdbc/execute! conn (str "truncate " (name table)))
      (doseq [batch batches]
        (copy/copy-into! (:connection conn) table cols (map ->tuple batch))))))


(defn bench [batched-data opts]
  (let [{:keys [table cols]} opts
        insert
        (do (println "jdbc/insert-multi!")
            (with-bench-tables
              (with-utc
                (crit/quick-benchmark
                 (bench-insert-multi table cols batched-data)
                 {}))))
        copy
        (do (println "clj-pgcopy")
            (with-bench-tables
              (with-utc
                (crit/quick-benchmark
                 (bench-pgcopy table cols batched-data)
                 {}))))]
    (assoc opts
           :insert-result (dissoc insert :runtime-details :os-details :options :results)
           :copy-result (dissoc copy :runtime-details :os-details :options :results))))

(defn bench-inventory [{:keys [total-rows batch-size]
                        :or {total-rows 1000
                             batch-size 100}}]
  (println (format "Typical data, %s, batches of %s" total-rows batch-size))
  (let [data (generate-inventory total-rows)
        table :benchmark.typical
        cols [:guid :created_at :active :price :average_rating]]
    (bench (partition-all batch-size data) {:table table
                                            :total-rows total-rows
                                            :batch-size batch-size
                                            :cols cols})))


(defn bench-binary-data [{:keys [total-rows batch-size]
                          :or {total-rows 1000
                               batch-size 100}
                          :as opts}]
  (println (format "Binary data, %s, batches of %s" total-rows batch-size))
  (let [data (generate-bindata total-rows)
        table :benchmark.bindata
        cols [:guid :payload]]
    (bench (partition-all batch-size data) {:table table
                                            :total-rows total-rows
                                            :batch-size batch-size
                                            :cols cols})))

(defn bench-textual-data [{:keys [total-rows batch-size]
                           :or {total-rows 1000
                                batch-size 100}}]
  (println (format "Textual data, %s, batches of %s" total-rows batch-size))
  (let [data (generate-textual total-rows)
        table :benchmark.textual
        cols [:guid :title :body]]
    (bench (partition-all batch-size data) {:table table
                                            :total-rows total-rows
                                            :batch-size batch-size
                                            :cols cols})))

(defn bench-fight-songs []
  (let [data (load-fight-songs)]
    (println (format "Fight song data, %s" (count data)))
    (let [table :benchmark.fight_songs
          cols [:school :conference :song_name :writers :year :student_writer :official_song :bpm :sec_duration :trope_count :fight :num_fights :spotify_id :victory :win_won :victory_win_won :rah :nonsense :colors :men :opponents]]
      (bench [data] {:table table
                     :batch-size 1
                     :cols cols}))))

(defn bench-all []
  (let [all (doall
             (for [[bench-fn total-rows] [[bench-inventory 5000]
                                          [bench-inventory 10000]
                                          [bench-inventory 20000]
                                          [bench-binary-data 5000]
                                          [bench-textual-data 10000]]
                   batch-size [250 1000]
                   :let [opts {:total-rows total-rows :batch-size batch-size}]]
               (bench-fn opts)))]
    (def all all)
    all))

(defn print-table [results]
  (crock/print-table [{:key-fn (fn [{:keys [table]}]
                                 (case table
                                   :benchmark.bindata "Binary"
                                   :benchmark.textual "Textual"
                                   :benchmark.typical "Typical"
                                   :benchmark.fight_songs "Fight Songs"
                                   (str table)))
                       :title "Table"}
                      {:key-fn :total-rows
                       :align :right
                       :title "Total"}
                      {:key-fn :batch-size
                       :align :right
                       :title "Batch"}
                      {:key-fn
                       #(format "%.1f" (* 1000 (get-in % [:insert-result :sample-mean 0])))
                       :align :right
                       :title "Insert"}
                      {:key-fn
                       #(format "%.1f" (* 1000 (get-in % [:copy-result :sample-mean 0])))
                       :align :right
                       :title "Copy"}]
                     results))

(defn -main [& _]
  (let [all (bench-all)]
    (print-table all)))


(comment
  (-main)
  (bench-fight-songs))
