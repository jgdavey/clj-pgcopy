(ns clj-pgcopy.core-test
  (:require [clj-pgcopy.core :as copy]
            [clj-pgcopy.time :as ptime]
            [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc])
  (:import (java.time LocalDateTime
                      LocalDate
                      Instant
                      ZoneId
                      ZoneOffset)
           (org.postgresql.util PGInterval)
           (org.postgresql.geometric PGbox
                                     PGcircle
                                     PGline
                                     PGpath
                                     PGpolygon
                                     PGpoint)
           (java.util TimeZone)))

(def conn-spec "jdbc:postgresql://localhost:5432/test_pgcopy")

(defprotocol IStringValue
  (string-value [_]))

(extend-protocol IStringValue
  String
  (string-value [this] this)
  nil
  (string-value [_] nil)
  org.postgresql.jdbc.PgSQLXML
  (string-value [this] (.getString this))
  Object
  (string-value [this] (str this)))

(use-fixtures :each
  (fn use-utc [f]
    (let [original (TimeZone/getDefault)]
      (TimeZone/setDefault (TimeZone/getTimeZone "Z"))
      (f)
      (TimeZone/setDefault original)))
  (fn create-test-tables [f]
    (jdbc/with-db-connection [conn conn-spec]
      (jdbc/execute! conn "drop schema if exists copytest cascade")
      (jdbc/execute! conn "create schema copytest")
      (jdbc/execute! conn "create extension if not exists citext")
      (jdbc/execute! conn (str "create table copytest.test("
                               "c_int integer,"
                               "c_bigint bigint,"
                               "c_smallint smallint,"
                               "c_text text,"
                               "c_varchar varchar(32),"
                               "c_char char(2),"
                               "c_citext citext,"
                               "c_text_array text[],"
                               "c_int_array int[],"
                               "c_double_array float8[],"
                               "c_uuid_array uuid[],"
                               "c_date date,"
                               "c_ts timestamp,"
                               "c_tstz timestamptz,"
                               "c_boolean boolean,"
                               "c_numeric numeric,"
                               "c_decimal decimal(8,2),"
                               "c_float4 float4,"
                               "c_float8 float8,"
                               "c_uuid uuid,"
                               "c_json json,"
                               "c_jsonb jsonb,"
                               "c_xml xml,"
                               "c_interval interval,"
                               "c_point point,"
                               "c_line line,"
                               "c_path path,"
                               "c_box box,"
                               "c_circle circle,"
                               "c_polygon polygon,"
                               "c_bytea bytea)"))
      (f)
      (jdbc/execute! conn  "drop schema if exists copytest cascade"))))

(deftest copy-columns-test
  (let [row1 {:c_smallint (short 42)
              :c_int (int 42)
              :c_bigint (* 4 Integer/MAX_VALUE)
              :c_text "Text"
              :c_varchar "Varchar"
              :c_char "ab"
              :c_citext "CIText"
              :c_text_array ["examples" "of" "text" nil]
              :c_int_array [1 2 3 4 5]
              :c_uuid_array [#uuid "cc7caf20-14e1-42de-a1e7-455d4f267111"
                             #uuid "cc7caf20-14e1-42de-a1e7-455d4f267112"
                             #uuid "cc7caf20-14e1-42de-a1e7-455d4f267113"]
              :c_double_array [1.2 3.4 5.6]
              :c_date (LocalDate/of 2018 1 2)
              :c_ts   (Instant/parse "2018-01-02T03:04:05.678Z")
              :c_tstz (Instant/parse "2018-01-02T03:04:05.678Z")
              :c_boolean true
              :c_numeric 123456789.012345M
              :c_decimal 4777.39M
              :c_float4 (float 3.14159)
              :c_float8 Math/PI
              :c_uuid #uuid "cc7caf20-14e1-42de-a1e7-455d4f267111"
              :c_json "{\"hello\": [1, 2, 3]}"
              :c_jsonb (copy/->JsonB "{\"goodbye\": [3, 2, 1]}")
              :c_xml "<fragment><stuff attr=\"yes\"></stuff></fragment>"
              :c_interval (PGInterval. 1 2 3 4 5 6.789)
              :c_point (PGpoint. "(1.2, -3.4)")
              :c_line (PGline. "{1.2, -3.4, 5.6}")
              :c_path (PGpath. "((1.2,-3.4),(-5.6,7.8),(9.0, 0.1))")
              :c_polygon (PGpolygon. "((1.2,-3.4),(-5.6,7.8),(9.0, 0.1))")
              :c_box (PGbox. "(-5.6,7.8),(1.2,-3.4)")
              :c_circle (PGcircle. "<(1.2,-3.4), 9.1>")
              :c_bytea "the byte array"}
        columns (-> row1 keys vec)
        data (vector row1 (into {} (map vector columns (repeat nil))))
        values (into [] (comp (map (fn [row] (-> row
                                                 (update :c_text_array #(into-array String %))
                                                 (update :c_int_array int-array)
                                                 (update :c_double_array double-array)
                                                 (update :c_uuid_array #(into-array java.util.UUID %))
                                                 (update :c_bytea #(if (string? %)
                                                                     (.getBytes % "UTF-8")
                                                                     %)))))
                              (map (apply juxt columns)))
                     data)
        bytes->string #(when (and % (pos? (count %))) (String. %))]
    (jdbc/with-db-connection [conn conn-spec]
      (is (= (count data)
             (copy/copy-into! (:connection conn) :copytest.test columns values)))
      (let [results (->> (jdbc/query conn ["select * from copytest.test"])
                         (map #(select-keys % columns )))]
        (is (= (into []
                     (comp
                      (map
                       (fn [row]
                         (-> row
                             (update :c_date ptime/to-instant)
                             (update :c_jsonb string-value))))
                      ;;(map (apply juxt columns))
                      )
                     data)
               (into []
                     (comp
                      (map (fn [row]
                             (-> row
                                 (update :c_date ptime/to-instant)
                                 (update :c_ts ptime/to-instant)
                                 (update :c_tstz ptime/to-instant)
                                 (update :c_bytea bytes->string)
                                 (update :c_text_array #(when % (not-empty (vec (.getArray %)))))
                                 (update :c_int_array #(when % (not-empty (vec (.getArray %)))))
                                 (update :c_double_array #(when % (not-empty (vec (.getArray %)))))
                                 (update :c_uuid_array #(when % (not-empty (vec (.getArray %)))))
                                 (update :c_citext string-value)
                                 (update :c_json string-value)
                                 (update :c_jsonb string-value)
                                 (update :c_xml string-value))))
                      ;;(map (apply juxt columns))
                      )
                     results)))))))

(deftest datetime-inputs-test
  (let [instant (Instant/parse "2018-01-02T03:04:05.678Z")
        data [{;; ZonedDateTime -> timestamp
               :c_ts (.atZone (LocalDateTime/ofInstant instant (ZoneId/of "Z"))
                              (ZoneId/of "Z"))
               ;; ZonedDateTime -> timestamptz
               :c_tstz (.atZone (LocalDateTime/ofInstant instant (ZoneId/of "Z"))
                                (ZoneId/of "Z"))}

              {;; OffsetDateTime -> timestamp
               :c_ts (.atOffset (LocalDateTime/ofInstant instant (ZoneId/of "Z"))
                                 ZoneOffset/UTC)
               ;; OffsetDateTime -> timestamptz
               :c_tstz (.atOffset (LocalDateTime/ofInstant instant (ZoneId/of "Z"))
                                  ZoneOffset/UTC)}

              {;; Instant -> timestamp
               :c_ts instant
               ;; Instant -> timestamptz
               :c_tstz instant}

              {;; java.util.Date -> timestamp
               :c_ts (java.util.Date/from instant)
               ;; java.util.Date -> timestamptz
               :c_tstz (java.util.Date/from instant)}

              {;; java.sql.Timestamp -> timestamp
               :c_ts (java.sql.Timestamp/from instant)
               ;; java.sql.Timestamp -> timestamptz
               :c_tstz (java.sql.Timestamp/from instant)}]
        columns (-> data first keys vec)
        values (into [] (map (apply juxt columns)) data)
        fixup-row (fn [row]
                    (-> row
                        (update :c_tstz ptime/to-instant)
                        (update :c_ts ptime/to-instant)))
        rows-xform (comp
                    (map fixup-row)
                    (map (apply juxt columns)))]
    (jdbc/with-db-connection [conn conn-spec]
      (is (= (count data)
             (copy/copy-into! (:connection conn) :copytest.test columns values)))
      (let [results (->> (jdbc/query conn ["select * from copytest.test"])
                         (map #(select-keys % columns)))]
        (is (= (into [] rows-xform data)
               (into [] rows-xform results)))))))

(deftest date-inputs-test
  (let [data [;; LocalDate -> date
              {:c_date (LocalDate/of 2018 1 2)}
              ;; java.sql.Date -> date
              {:c_date (java.sql.Date/valueOf (LocalDate/of 2018 1 2))}
              ;; Far past dates
              {:c_date (LocalDate/of 1980 1 2)}
              {:c_date (java.sql.Date/valueOf (LocalDate/of 1582 10 4))}
              {:c_date (java.sql.Date/valueOf (LocalDate/of 720 5 19))}
              {:c_date (LocalDate/of 2 2 2)}
              ;; Far future dates
              {:c_date (LocalDate/of 4242 1 2)}]
        columns (-> data first keys vec)
        values (into [] (map (apply juxt columns)) data)
        fixup-row (fn [row]
                    (-> row
                        (update :c_date ptime/to-instant)))
        rows-xform (comp
                    (map fixup-row)
                    (map (apply juxt columns)))]
    (jdbc/with-db-connection [conn conn-spec]
      (is (= (count data)
             (copy/copy-into! (:connection conn) :copytest.test columns values)))
      (let [results (->> (jdbc/query conn ["select * from copytest.test"])
                         (map #(select-keys % columns)))]
        (is (= (into [] rows-xform data)
               (into [] rows-xform results)))))))
