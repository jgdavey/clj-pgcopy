(ns clj-pgcopy.core
  (:require [clojure.string :as str]
            [clj-pgcopy.time :as ptime])
  (:import (java.io ByteArrayInputStream
                    ByteArrayOutputStream
                    BufferedReader
                    BufferedOutputStream
                    DataOutputStream)
           (java.nio ByteBuffer)
           (java.time LocalDateTime
                      LocalDate
                      ZonedDateTime
                      OffsetDateTime
                      Instant)
           (java.util UUID)
           (org.postgresql.geometric PGbox
                                     PGcircle
                                     PGline
                                     PGpath
                                     PGpolygon
                                     PGpoint)
           (org.postgresql.util PGInterval)
           (org.postgresql.copy CopyManager
                                CopyIn
                                PGCopyOutputStream)
           (org.postgresql.jdbc PgConnection)
           (org.postgresql.core BaseConnection)))

(set! *warn-on-reflection* true)

(defprotocol IPGBinaryWrite
  (pg-type [this])
  (write-to [this ^DataOutputStream out]))

(def oids
  {:bytea 17
   :text 25
   :int4 23
   :int8 20
   :int2 21
   :char 18
   :boolean 16
   :jsonb 114
   :xml 115
   :point 600
   :line 628
   :path 602
   :box 603
   :polygon 604
   :circle 705
   :float4 700
   :float8 701
   :unknown 705
   :varchar 1043
   :date 1082
   :timestamp 1114
   :timestamptz 1184
   :interval 1186
   :numeric 1700
   :uuid 2950})

(defn array->bytes
  "Returns binary-encoded byte array when type of array can be determined, or nil"
  [pg-type coll]
  (let [baos ^ByteArrayOutputStream (ByteArrayOutputStream. 1024)
        [_ oid] (find oids pg-type)
        coll (seq coll)]
    (when oid
      (with-open [out ^DataOutputStream (DataOutputStream. baos)]
        (.writeInt out 1)            ;; dimensions (only 1 dimensional)
        (.writeInt out 1)            ;; nullable values allowed
        (.writeInt out oid)          ;; oid of collection elements
        (.writeInt out (count coll)) ;; size
        (.writeInt out 1)            ;; use PG default
        (doseq [el coll]
          (write-to el out)))
      (.toByteArray baos))))

;; Inspired by Java impl here:
;; https://github.com/bytefish/PgBulkInsert/blob/master/PgBulkInsert/src/main/java/de/bytefish/pgbulkinsert/pgsql/handlers/BigDecimalValueHandler.java
(defn numeric-components [^BigDecimal bd]
  (let [unscaled ^BigInteger (.unscaledValue bd)
        sign (.signum bd)
        unscaled (if (= -1 sign) (.negate unscaled) unscaled)
        fraction-digits ^int (.scale bd)
        fraction-groups (unchecked-divide-int (unchecked-add-int fraction-digits (int 3))
                                              4)
        scale-remainder (mod fraction-digits 4)
        [unscaled digits] (if (zero? scale-remainder)
                            [unscaled (list)]
                            ;; scale the first value
                            (let [result (.divideAndRemainder unscaled (.pow (BigInteger. "10") scale-remainder))
                                  digit (unchecked-multiply-int ^int (.intValue ^BigInteger (aget result 1))
                                                                (int (Math/pow 10 (- 4 scale-remainder))))]
                              [(aget result 0) (list digit)]))
        digits (loop [^BigInteger unscaled unscaled
                      digits digits]
                 (if (.equals unscaled BigInteger/ZERO)
                   digits
                   (let [result (.divideAndRemainder unscaled (BigInteger. "10000"))]
                     (recur
                      (aget result 0)
                      (cons (.intValue ^BigInteger (aget result 1)) digits)))))]
    {:sign sign
     :fraction-groups fraction-groups
     :fraction-digits fraction-digits
     :digits digits}))

(extend-protocol IPGBinaryWrite
  (Class/forName "[B")
  (pg-type [_] :bytea)
  (write-to [ba ^DataOutputStream out]
    (.writeInt out (count ba))
    (.write out ^bytes ba))

  String
  (pg-type [_] :text)
  (write-to [string ^DataOutputStream out]
    (write-to ^bytes (.getBytes string "UTF-8") out))

  Short
  (pg-type [_] :int2)
  (write-to [sh ^DataOutputStream out]
    (.writeInt out 2)
    (.writeShort out sh))

  Integer
  (pg-type [_] :int4)
  (write-to [integer ^DataOutputStream out]
    (.writeInt out 4)
    (.writeInt out integer))

  Long
  (pg-type [_] :int8)
  (write-to [num ^DataOutputStream out]
    (.writeInt out 8)
    (.writeLong out num))

  Float
  (pg-type [_] :float4)
  (write-to [f ^DataOutputStream out]
    (.writeInt out 4)
    (.writeFloat out (.floatValue f)))

  Double
  (pg-type [_] :float8)
  (write-to [d ^DataOutputStream out]
    (.writeInt out 8)
    (.writeDouble out (.doubleValue d)))

  BigDecimal
  (pg-type [_] :numeric)
  (write-to [bd ^DataOutputStream out]
    (let [{:keys [fraction-digits fraction-groups sign digits]}
          (numeric-components bd)
          n-digits (int (count digits))]
      (.writeInt out (int (+ 8 (* 2 n-digits))))
      (.writeShort out n-digits)
      (.writeShort out (- n-digits fraction-groups 1))
      (.writeShort out (int (if (= sign 1) 0x0000 0x4000)))
      (.writeShort out fraction-digits)
      (doseq [digit digits]
        (.writeShort out (int digit)))))

  Boolean
  (pg-type [_] :boolean)
  (write-to [bool ^DataOutputStream out]
    (.writeInt out 1)
    (.writeByte out (if bool 1 0)))

  PGInterval
  (pg-type [_] :interval)
  (write-to [interval ^DataOutputStream out]
    (.writeInt out 16)
    (let [secs (.getSeconds interval)
          mins (double (.getMinutes interval))
          hours (double (.getHours interval))
          seconds (+ secs (* 60 mins) (* 60 60 hours))
          days (.getDays interval)
          months (.getMonths interval)
          years (.getYears interval)]
      (.writeLong out (long (* 1000000 seconds)))
      (.writeInt out days)
      (.writeInt out (+ months (* 12 years)))))

  java.util.Date
  (pg-type [_] :timestamp)
  (write-to [date ^DataOutputStream out]
    (write-to (ptime/java-epoch->postgres-epoch (.getTime date)) out))

  Instant
  (pg-type [_] :timestamp)
  (write-to [instant ^DataOutputStream out]
    (write-to (ptime/java-epoch->postgres-epoch (.toEpochMilli instant)) out))

  LocalDate
  (pg-type [_] :date)
  (write-to [date ^DataOutputStream out]
    (let [days (-> date
                   .atStartOfDay
                   ptime/date-time->epoch-milli
                   ptime/java-epoch->postgres-days
                   int)]
      (.writeInt out 4)
      (.writeInt out days)))

  java.sql.Date
  (pg-type [_] :date)
  (write-to [date ^DataOutputStream out]
    (write-to ^LocalDate (.toLocalDate date) out))

  java.sql.Timestamp
  (pg-type [_] :timestamp)
  (write-to [ts ^DataOutputStream out]
    (write-to ^Instant (.toInstant ts) out))

  ;; Do not assume UTC, needs a time zone or offset
  ;; LocalDateTime
  ;; (pg-type [_] :timestamp)
  ;; (write-to [ldt ^DataOutputStream out]
  ;;   (write-to ^Instant (.toInstant ldt) out))

  ZonedDateTime
  (pg-type [_] :timestamp)
  (write-to [zdt ^DataOutputStream out]
    (write-to ^Instant (.toInstant zdt) out))

  OffsetDateTime
  (pg-type [_] :timestamp)
  (write-to [odt ^DataOutputStream out]
    (write-to ^Instant (.toInstant odt) out))

  PGpoint
  (pg-type [_] :point)
  (write-to [p ^DataOutputStream out]
    (.writeInt out 16)
    (.writeDouble out (.-x p))
    (.writeDouble out (.-y p)))

  PGline
  (pg-type [_] :line)
  (write-to [line ^DataOutputStream out]
    (.writeInt out 24)
    (.writeDouble out (.-a line))
    (.writeDouble out (.-b line))
    (.writeDouble out (.-c line)))

  PGpath
  (pg-type [_] :path)
  (write-to [p ^DataOutputStream out]
    (let [points (.-points p)
          closed (byte (if (.-open p) 0 1))
          byte-count (+ 1  ;; open/closed
                        4  ;; number of points
                        (* 16 (count points)) ;; point data
                        )]
      (.writeInt out byte-count)
      (.writeByte out closed)
      (.writeInt out (count points))
      (doseq [^PGpoint point points]
        (.writeDouble out (.-x point))
        (.writeDouble out (.-y point)))))

  PGpolygon
  (pg-type [_] :polygon)
  (write-to [p ^DataOutputStream out]
    (let [points (.-points p)
          byte-count (+ 4 (* 16 (count points)))]
      (.writeInt out byte-count)
      (.writeInt out (count points))
      (doseq [^PGpoint point points]
        (.writeDouble out (.-x point))
        (.writeDouble out (.-y point)))))

  PGbox
  (pg-type [_] :box)
  (write-to [box ^DataOutputStream out]
    (let [points (.-point box)]
      (.writeInt out 32)
      (doseq [^PGpoint point points]
        (.writeDouble out (.-x point))
        (.writeDouble out (.-y point)))))

  PGcircle
  (pg-type [_] :circle)
  (write-to [c ^DataOutputStream out]
    (let [center ^PGpoint (.-center c)]
      (.writeInt out 24)
      (.writeDouble out (.-x center))
      (.writeDouble out (.-y center))
      (.writeDouble out (.-radius c))))

  ;; TODO Is this a good idea?
  ;; clojure.lang.APersistentVector
  ;; (pg-type [_] nil) ;; array's use sub oid
  ;; (write-to [this ^DataOutputStream out]
  ;;   (if (and (seq this) (satisfies? IPGBinaryWrite (first this)))
  ;;     (if-some [ba ^bytes (array->bytes (pg-type (first this)) this)]
  ;;       (do
  ;;         (.writeInt out (count ba))
  ;;         (.write out ba))
  ;;       (write-to nil out))
  ;;     (write-to nil out)))

  UUID
  (pg-type [_] :uuid)
  (write-to [uuid ^DataOutputStream out]
    (let [bb ^ByteBuffer (ByteBuffer/wrap (byte-array 16))]
      (.putLong bb (.getMostSignificantBits uuid))
      (.putLong bb (.getLeastSignificantBits uuid))
      (.writeInt out 16)
      (.writeInt out (.getInt bb 0))
      (.writeShort out (.getShort bb 4))
      (.writeShort out (.getShort bb 6))
      (.writeLong out (.getLong bb 8))))

  nil
  (oid [_] :unknown)
  (write-to [_ ^DataOutputStream out]
    (.writeInt out -1)))

;; These primitive array impls have to be added separate or the
;; compiler complains

(defmacro extend-primitive-array
  [klass pg-type]
  `(extend-type ~klass
     IPGBinaryWrite
     (pg-type [~'_] nil)
     (write-to [this# ^DataOutputStream out#]
       (if-some [ba# ^{:tag ~'bytes} (array->bytes ~pg-type this#)]
         (do
           (.writeInt out# (count ba#))
           (.write out# ba#))
         (write-to nil out#)))))

 ;; int array
(extend-primitive-array (Class/forName "[I") :int4)

 ;; long array
(extend-primitive-array (Class/forName "[J") :int8)

;; float array
(extend-primitive-array (Class/forName "[F") :float4)

;; double array
(extend-primitive-array (Class/forName "[D") :float8)

;; String array
(extend-primitive-array (Class/forName "[Ljava.lang.String;") :text)

(extend-primitive-array (Class/forName "[Ljava.util.UUID;") :uuid)

(deftype JsonB [^String value]
  Object
  (toString [_] value)
  IPGBinaryWrite
  (pg-type [_] :jsonb)
  (write-to [_ o]
    (let [out ^DataOutputStream o]
      (if (str/blank? value)
        (write-to out nil)
        (let [ba ^bytes (.getBytes value "UTF-8")]
          (.writeInt out (+ 1 (count ba)))
          (.writeByte out 1) ;; jsonb protocol version
          (.write out ba))))))

(defn copy-to-stream [^PGCopyOutputStream stream tuples]
  (with-open [out ^DataOutputStream (DataOutputStream. (BufferedOutputStream. stream 65536))]
    ;; constant header
    (.writeBytes out "PGCOPY\n\377\r\n\0")
    ;; header flags (no OIDs)
    (.writeInt out 0)
    ;; header extension (unused)
    (.writeInt out 0)
    (doseq [tuple tuples]
      (.writeShort out (count tuple))
      (doseq [field tuple]
        (write-to field out)))
    ;; footer constant
    (.writeShort out (short -1))
    (.flush out)))

(defn values->copy-rows-binary ^bytes [values]
  (with-open [bout ^ByteArrayOutputStream (ByteArrayOutputStream. 4096)]
    (copy-to-stream bout values)
    (.toByteArray bout)))

(defn- ^PgConnection unwrap-connection [^java.sql.Connection conn]
  (if (.isWrapperFor conn BaseConnection)
    (.unwrap conn BaseConnection)
    conn))

(defn copy-table
  ([conn table]
   (copy-table conn table {:csv? true :headers? false}))
  ([^java.sql.Connection conn table {:keys [csv? headers?]}]
   (let [conn (unwrap-connection conn)
         manager ^CopyManager (.getCopyAPI conn)
         copy-query (str "COPY " table " TO STDOUT"
                         (when csv? " WITH CSV")
                         (when headers? " HEADER"))]
     (with-open [out (ByteArrayOutputStream.)]
       (.copyOut manager copy-query out)
       (String. (.toByteArray out))))))

(defn copy-into!
  "table-sql is the table name and columns for the COPY statement,
  e.g. myschema.mytable(col1, col2). It should match the order of the
  tuples exactly."
  ([^java.sql.Connection conn
    table-sql
    values]
   (let [conn (unwrap-connection conn)
         manager ^CopyManager (.getCopyAPI conn)
         cmd ^String (str "COPY " (name table-sql) " FROM STDIN WITH BINARY")
         op (.copyIn manager cmd)]
     ;; using BufferedOutputStream's buffer, so keep this one small
     (with-open [out (PGCopyOutputStream. op 1)]
       (copy-to-stream out values))
     (.getHandledRowCount op)))
  ([^java.sql.Connection conn table cols values]
   (let [table-spec (str (name table)
                         "("
                         (str/join "," (map name cols))
                         ")")]
     (copy-into! conn table-spec values))))
