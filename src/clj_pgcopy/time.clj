(ns clj-pgcopy.time
  (:import (java.time LocalDateTime
                      LocalDate
                      ZoneOffset
                      ZonedDateTime
                      OffsetDateTime
                      Instant)
           (java.util.concurrent TimeUnit)))

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
  (-to-instant [self]))

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

(defn to-instant [date-ish]
  (-to-instant date-ish))
