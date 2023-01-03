(ns clj-pgcopy.impl
  (:require
   [clj-pgcopy.protocols :as proto])
  (:import
   (java.io ByteArrayOutputStream DataOutputStream)
   (java.net Inet4Address Inet6Address)
   (java.nio ByteBuffer)
   (java.time
    Instant
    LocalDate
    LocalDateTime
    OffsetDateTime
    ZoneOffset
    ZonedDateTime)
   (java.util UUID)
   (java.util.concurrent TimeUnit)
   (org.postgresql.geometric
    PGbox
    PGcircle
    PGline
    PGpath
    PGpoint
    PGpolygon)
   (org.postgresql.util PGInterval)))

(set! *warn-on-reflection* true)

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

;; Inspired by Java impl here:
;; https://github.com/bytefish/PgBulkInsert/blob/master/PgBulkInsert/src/main/java/de/bytefish/pgbulkinsert/pgsql/handlers/BigDecimalValueHandler.java
(defn numeric-components [^BigDecimal bd]
  (let [unscaled ^BigInteger (.unscaledValue bd)
        sign (.signum bd)
        unscaled (if (= -1 sign) (.negate unscaled) unscaled)
        fraction-digits ^int (.scale bd)]
    (if (> fraction-digits 0)
      (let [fraction-groups (unchecked-divide-int (unchecked-add-int fraction-digits (int 3))
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
         :digits digits})
      (let [original-value (.multiply unscaled (.pow (BigInteger. "10") (Math/abs fraction-digits)))
            digits (loop [^BigInteger value original-value
                          digits []]
                     (if (.equals value BigInteger/ZERO)
                       digits
                       (let [result (.divideAndRemainder value (BigInteger. "10000"))]
                         (recur
                          (aget result 0)
                          (cons (.intValue ^BigInteger (aget result 1)) digits)))))]
        {:sign sign
         :fraction-groups 0
         :fraction-digits 0
         :digits digits}))))

(comment
  (numeric-components (BigDecimal. "1.2E34"))
  (numeric-components (BigDecimal. "3.4E-21"))
  (numeric-components (BigDecimal. "1000"))
  (numeric-components (BigDecimal. "0.01"))
  (numeric-components (BigDecimal. "0.00002"))
  (numeric-components (BigDecimal. "1.00002"))
  (numeric-components (BigDecimal. "1.0002"))

  (.scale (BigDecimal. "1.00001"))
  (.scale (BigDecimal. "0.00001"))
  )


(extend-protocol proto/IPGBinaryWrite
  ;; bytea
  (Class/forName "[B")
  (write-to [ba ^DataOutputStream out]
    (.writeInt out (count ba))
    (.write out ^bytes ba))

  ;; text
  String
  (write-to [string ^DataOutputStream out]
    (proto/write-to ^bytes (.getBytes string "UTF-8") out))

  ;; int4
  Short
  (write-to [sh ^DataOutputStream out]
    (.writeInt out 2)
    (.writeShort out sh))

  ;; int4
  Integer
  (write-to [integer ^DataOutputStream out]
    (.writeInt out 4)
    (.writeInt out integer))

  ;; int8
  Long
  (write-to [num ^DataOutputStream out]
    (.writeInt out 8)
    (.writeLong out num))

  ;; float4
  Float
  (write-to [f ^DataOutputStream out]
    (.writeInt out 4)
    (.writeFloat out (.floatValue f)))

  ;; float8
  Double
  (write-to [d ^DataOutputStream out]
    (.writeInt out 8)
    (.writeDouble out (.doubleValue d)))

  ;; numeric
  BigDecimal
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

  ;; boolean
  Boolean
  (write-to [bool ^DataOutputStream out]
    (.writeInt out 1)
    (.writeByte out (if bool 1 0)))

  ;; interval
  PGInterval
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

  ;; point
  PGpoint
  (write-to [p ^DataOutputStream out]
    (.writeInt out 16)
    (.writeDouble out (.-x p))
    (.writeDouble out (.-y p)))

  ;; line
  PGline
  (write-to [line ^DataOutputStream out]
    (.writeInt out 24)
    (.writeDouble out (.-a line))
    (.writeDouble out (.-b line))
    (.writeDouble out (.-c line)))

  ;; path
  PGpath
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

  ;; polygon
  PGpolygon
  (write-to [p ^DataOutputStream out]
    (let [points (.-points p)
          byte-count (+ 4 (* 16 (count points)))]
      (.writeInt out byte-count)
      (.writeInt out (count points))
      (doseq [^PGpoint point points]
        (.writeDouble out (.-x point))
        (.writeDouble out (.-y point)))))

  ;; box
  PGbox
  (write-to [box ^DataOutputStream out]
    (let [points (.-point box)]
      (.writeInt out 32)
      (doseq [^PGpoint point points]
        (.writeDouble out (.-x point))
        (.writeDouble out (.-y point)))))

  ;; circle
  PGcircle
  (write-to [c ^DataOutputStream out]
    (let [center ^PGpoint (.-center c)]
      (.writeInt out 24)
      (.writeDouble out (.-x center))
      (.writeDouble out (.-y center))
      (.writeDouble out (.-radius c))))

  ;; uuid
  UUID
  (write-to [uuid ^DataOutputStream out]
    (let [bb ^ByteBuffer (ByteBuffer/wrap (byte-array 16))]
      (.putLong bb (.getMostSignificantBits uuid))
      (.putLong bb (.getLeastSignificantBits uuid))
      (.writeInt out 16)
      (.writeInt out (.getInt bb 0))
      (.writeShort out (.getShort bb 4))
      (.writeShort out (.getShort bb 6))
      (.writeLong out (.getLong bb 8))))

  ;; inet
  Inet4Address
  (write-to [value ^DataOutputStream out]
    (.writeInt out 8)
    (.writeByte out 2)
    (.writeByte out 32)
    (.writeByte out 0)
    (let [addr ^bytes (.getAddress value)]
      (.writeByte out (count addr))
      (.write out addr)))

  ;; inet
  Inet6Address
  (write-to [value ^DataOutputStream out]
    (.writeInt out 20)
    (.writeByte out 3)
    (.writeByte out 128)
    (.writeByte out 0)
    (let [addr ^bytes (.getAddress value)]
      (.writeByte out (count addr))
      (.write out addr)))

  ;; null
  nil
  (pg-type [_] :unknown)
  (write-to [_ ^DataOutputStream out]
    (.writeInt out -1)))

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
          (proto/write-to el out)))
      (.toByteArray baos))))

;; These primitive array impls have to be added separate or the
;; compiler complains

(defmacro extend-primitive-array
  [klass pg-type]
  `(extend-type ~klass
     proto/IPGBinaryWrite
     (write-to [this# ^DataOutputStream out#]
       (if-some [ba# ^{:tag ~'bytes} (array->bytes ~pg-type this#)]
         (do
           (.writeInt out# (count ba#))
           (.write out# ba#))
         (proto/write-to nil out#)))))

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

;; UUID array
(extend-primitive-array (Class/forName "[Ljava.util.UUID;") :uuid)


;; Dates and Timestamps

(defn date-time->epoch-milli ^Long [^LocalDateTime dt]
  (.. dt
      (atOffset ZoneOffset/UTC)
      toInstant
      toEpochMilli))

(def epoch-distance
  (-
   (date-time->epoch-milli
    ;; postgres epoch
    (LocalDateTime/of 2000 1 1 0 0 0))
   (.toEpochMilli Instant/EPOCH)))

;; 946684800000

;; The conversion is valid for any year 1583 CE onwards.
(defn java-epoch->postgres-epoch ^long [millis]
  ;; returns microseconds
  (long (* 1000 (- millis epoch-distance))))

(defn java-epoch->postgres-days [millis]
  (->> (- millis epoch-distance)
       (.toDays TimeUnit/MILLISECONDS)
       int))

(defprotocol ToInstant
  (-to-instant ^Instant [self]))

(extend-protocol ToInstant
  LocalDateTime
  (-to-instant [self]
    (.. self
        (atOffset ZoneOffset/UTC)
        toInstant))
  LocalDate
  (-to-instant [self]
    (.. self
        atStartOfDay
        (atOffset ZoneOffset/UTC)
        toInstant))
  Instant
  (-to-instant [self]
    self)
  java.util.Date
  (-to-instant [self]
    (.toInstant self))
  java.sql.Date
  (-to-instant [self]
    (-to-instant
     (.toLocalDate self)))
  java.sql.Timestamp
  (-to-instant [self]
    (.toInstant self))
  ZonedDateTime
  (-to-instant [self]
    (.toInstant self))
  OffsetDateTime
  (-to-instant [self]
    (.toInstant self))
  nil
  (-to-instant [_]
    nil))

(defn write-timestamp [obj ^DataOutputStream out]
  (.writeInt out 8)
  (.writeLong out (java-epoch->postgres-epoch (.toEpochMilli (-to-instant obj)))))

(extend java.util.Date
  proto/IPGBinaryWrite {:write-to write-timestamp})

(extend Instant
  proto/IPGBinaryWrite {:write-to write-timestamp})

(extend java.sql.Timestamp
  proto/IPGBinaryWrite {:write-to write-timestamp})

(extend ZonedDateTime
  proto/IPGBinaryWrite {:write-to write-timestamp})

(extend OffsetDateTime
  proto/IPGBinaryWrite {:write-to write-timestamp})

(extend-type LocalDate
  proto/IPGBinaryWrite
  (write-to [date ^DataOutputStream out]
    (let [days (-> date
                   -to-instant
                   (.toEpochMilli)
                   java-epoch->postgres-days
                   int)]
      (.writeInt out 4)
      (.writeInt out days))))

(extend-type java.sql.Date
  proto/IPGBinaryWrite
  (write-to [date ^DataOutputStream out]
    (proto/write-to ^LocalDate (.toLocalDate date) out)))
