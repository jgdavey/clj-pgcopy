(ns clj-pgcopy.core
  (:require [clojure.string :as str]
            [clj-pgcopy.protocols :as proto]
            clj-pgcopy.impl)
  (:import (java.io ByteArrayOutputStream
                    BufferedOutputStream
                    DataOutputStream
                    OutputStream)
           (org.postgresql.copy CopyManager
                                PGCopyOutputStream)
           (org.postgresql.jdbc PgConnection)
           (org.postgresql.core BaseConnection)))

(set! *warn-on-reflection* true)

(deftype JsonB [^String value]
  Object
  (toString [_] value)
  proto/IPGBinaryWrite
  (write-to [_ o]
    (let [out ^DataOutputStream o]
      (if (str/blank? value)
        (proto/write-to out nil)
        (let [ba ^bytes (.getBytes value "UTF-8")]
          (.writeInt out (+ 1 (count ba)))
          (.writeByte out 1) ;; jsonb protocol version
          (.write out ba))))))

(defn copy-to-stream [^OutputStream stream tuples opts]
  (with-open [out ^DataOutputStream (DataOutputStream. (BufferedOutputStream. stream (get opts :buffer-size 65536)))]
    ;; constant header
    (.writeBytes out "PGCOPY\n\377\r\n\0")
    ;; header flags (no OIDs)
    (.writeInt out 0)
    ;; header extension (unused)
    (.writeInt out 0)
    (doseq [tuple tuples]
      (.writeShort out (count tuple))
      (doseq [field tuple]
        (proto/write-to field out)))
    ;; footer constant
    (.writeShort out (short -1))
    (.flush out)))

(defn values->copy-rows-binary ^bytes [values]
  (with-open [bout ^ByteArrayOutputStream (ByteArrayOutputStream. 4096)]
    (copy-to-stream bout values {})
    (.toByteArray bout)))

(defn- unwrap-connection ^PgConnection [^java.sql.Connection conn]
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

(defn to-table-spec [table cols]
  (str (name table)
       "("
       (str/join "," (map name cols))
       ")"))

(defn copy-values-into!
  "table-sql is the table name and columns for the COPY statement,
  e.g. myschema.mytable(col1, col2). It should match the order of the
  tuples exactly."
  ([conn table-sql values]
   (copy-values-into! conn table-sql values nil))
  ([^java.sql.Connection conn
    table-sql
    values
    opts]
   (let [conn (unwrap-connection conn)
         manager ^CopyManager (.getCopyAPI conn)
         cmd ^String (str "COPY " (name table-sql) " FROM STDIN WITH BINARY")
         op (.copyIn manager cmd)]
     ;; using BufferedOutputStream's buffer, so keep this one small
     (with-open [out (PGCopyOutputStream. op 1)]
       (copy-to-stream out values opts))
     (.getHandledRowCount op))))

(defn ^{:deprecated "0.2.0"} copy-into!
  ([^java.sql.Connection conn table-spec values]
   (copy-values-into! conn table-spec values nil))
  ([^java.sql.Connection conn table cols values]
   (let [table-spec (to-table-spec table cols)]
     (copy-values-into! conn table-spec values nil))))

(defn copy-into-table!
  ([conn table cols values]
   (copy-into-table! conn table cols values nil))
  ([^java.sql.Connection conn table cols values opts]
   (let [table-spec (to-table-spec table cols)]
     (copy-values-into! conn table-spec values opts))))
