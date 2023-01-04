#+TITLE: clj-pgcopy

#+html: <a href="https://clojars.org/clj-pgcopy"><img src="https://img.shields.io/clojars/v/clj-pgcopy.svg" alt="Clojars" /></a>

Import data into postgres quickly, implemented using postgresql's
=COPY= in binary format.

Because sometimes =jdbc/insert!= and friends aren't fast enough.

This library uses type-based dispatch for determining the correct
postgres binary format. See the [[Input Type mapping][mapping]] section for more info.

* Usage

Add the library to your project. See [[https://clojars.org/clj-pgcopy][description on Clojars]] for more
information.

#+BEGIN_SRC clojure :exports none
(require '[clojure.java.jdbc :as jdbc])

(def conn-spec "jdbc:postgresql://localhost:5432/test_pgcopy")
#+END_SRC

#+RESULTS:
: nil#'user/conn-spec

Given a table and some data:

#+BEGIN_SRC clojure :exports code
(require '[clj-pgcopy.core :as pgcopy])

(jdbc/with-db-connection [conn conn-spec]
  (jdbc/db-do-commands conn
                       ["drop table if exists example"
                        "create table example(
  internal_id bigint primary key,
  external_id uuid,
  height float8,
  title varchar(64) not null,
  description text,
  values bytea,
  dob date,
  created_at timestamptz
)"]))

(def data
  [{:internal_id 201223123
    :external_id #uuid "1902c205-2bc6-40b8-943b-f5b199241316"
    :height nil
    :title "Mr. Sandman"
    :description nil
    :values (.getBytes "not very secret" "UTF-8")
    :dob (java.time.LocalDate/of 1954 8 20)
    :created_at (java.util.Date.)}
   {:internal_id 2012391238
    :external_id nil
    :height 160.2
    :title "Prince"
    :description "Tonight we're gonna party"
    :values (.getBytes "2000 Two Zero" "UTF-8")
    :dob (java.time.LocalDate/of 1999 12 31)
    :created_at (java.util.Date.)}])

#+END_SRC

#+RESULTS:
: nil(0 0)#'user/data

With =clojure.java.jdbc=, open a connection, prepare data rows (as
tuples, not maps), and call =clj-pgcopy.core/copy-into!=:

#+BEGIN_SRC clojure :exports both
(let [columns [:internal_id :external_id :height
               :title :description :values :dob :created_at]]
  (jdbc/with-db-connection [conn conn-spec]
    (pgcopy/copy-into! (:connection conn)
                       :example
                       columns
                       (map (apply juxt columns) data))))
#+END_SRC

#+RESULTS:
: 2

The table has been populated with the data:

#+BEGIN_SRC clojure :exports both :results pp
(jdbc/with-db-connection [conn conn-spec]
  (jdbc/query conn "table example"))
#+END_SRC

#+RESULTS:
#+begin_example
({:internal_id 201223123,
  :external_id #uuid "1902c205-2bc6-40b8-943b-f5b199241316",
  :height nil,
  :title "Mr. Sandman",
  :description nil,
  :values
  [110, 111, 116, 32, 118, 101, 114, 121, 32, 115, 101, 99, 114, 101,
   116],
  :dob #inst "1954-08-20T04:00:00.000-00:00",
  :created_at #inst "2019-07-23T01:24:38.466000000-00:00"}
 {:internal_id 2012391238,
  :external_id nil,
  :height 160.2,
  :title "Prince",
  :description "Tonight we're gonna party",
  :values [50, 48, 48, 48, 32, 84, 119, 111, 32, 90, 101, 114, 111],
  :dob #inst "1999-12-31T05:00:00.000-00:00",
  :created_at #inst "2019-07-23T01:24:38.466000000-00:00"})
#+end_example

Note: depending on how you've set up =clojure.java.jdbc= and its
=IResultSetReadColumn= protocol, the types that come back on query may
differ from the above.

* Input Type mapping

** Basic type mapping

| JVM type       | Postgres type                 |
|----------------+-------------------------------|
| Short          | int2 (aka smallint)           |
| Integer        | int4 (aka integer)            |
| Long           | int8 (aka bigint)             |
| Float          | float4 (aka real)             |
| Double         | float8 (aka double presicion) |
| BigDecimal     | numeric/decimal               |
| Boolean        | boolean                       |
| String         | text/varchar/char             |
| java.util.UUID | uuid                          |

** Date-related mappings

| JVM type                     | Postgres type |
|------------------------------+---------------|
| java.sql.Date                | date          |
| java.time.LocalDate          | date          |
| java.util.Date               | timestamp[tz] |
| java.sql.Timestamp           | timestamp[tz] |
| java.time.Instant            | timestamp[tz] |
| java.time.ZonedDateTime      | timestamp[tz] |
| java.time.OffsetDatetime     | timestamp[tz] |
| org.postgres.util.PGInterval | interval      |

** Other JVM native types

| JVM type              | Postgres type                 |
|-----------------------+-------------------------------|
| java.net.Inet4Address | inet                          |
| java.net.Inet6Address | inet                          |

** Geometric mappings

| JVM type                         | Postgres type |
|----------------------------------+---------------|
| org.postgres.geometric.PGpoint   | point         |
| org.postgres.geometric.PGline    | line          |
| org.postgres.geometric.PGpath    | path          |
| org.postgres.geometric.PGbox     | box           |
| org.postgres.geometric.PGcircle  | circle        |
| org.postgres.geometric.PGpolygon | polygon       |

Things that are String-like, or serialized in string form, should work
using the String -> text mapping. An exception is the =jsonb= type,
because the binary format requires a version signifier. Wrapping a
JSON string in a =JsonB= handles that, which is provided by the
library.

** Arrays

Impemented for the following JVM-typed arrays for:

| JVM type         | Postgres type                     |
|------------------+-----------------------------------|
| int[]            | int4[] (aka integer[])            |
| long[]           | int8[] (aka bigint[])             |
| float[]          | float4[] (aka real[])             |
| double[]         | float8[] (aka double precision[]) |
| byte[]           | bytea                             |
| String[]         | text[] (or varchar[])             |
| java.util.UUID[] | uuid[]                            |


Currently, only 1-dimensional Postgres arrays are supported.

** Planned But Not Yet Implemented

- more array types (date, timestamp, etc)
- range types

** Unplanned

- hstore
- cidr, macaddr, macaddr8
- bit strings
- composite types / records
- multi-dimensional arrays

