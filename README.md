# `kadb_fdw`: ADB / GPDB foreign data wrapper for transactional data load from Kafka
`kadb_fdw` is an extension for ADB / GPDB that implements transactional data load from Kafka.

Features:
* Storing [Kafka offsets](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) outside of Kafka, in ADB / GPDB
* Support of ADB / GPDB transactions
* [AVRO](https://avro.apache.org/), and CSV [deserialization](#deserialization)
* Support of Kerberos authentication


## Installation
`kadb_fdw` is shipped in PostgreSQL extension format, and follows the common extension pipeline.

Note that extension installation SQL script includes
```sql
RESET client_min_messages;
```

### Dependencies
Currently, there are the following shared-library dependencies which must be met:
* [librdkafka](https://github.com/edenhill/librdkafka). Tested with:
    * `0.11.4`
    * `1.3.0`
    * `1.5.0`
* [libavro-c](https://github.com/apache/avro/tree/master/lang/c). Tested with:
    * `1.7.7`
        * On CentOS, newer versions *must* link `jansson` library **statically**. `libavro-c` does not provide an easy way to do that; thus `1.7.7` is currently the recommended version.
    * `1.8.0`
    * `1.9.0`, `1.9.2`
* [libcsv](https://github.com/rgamble/libcsv). Tested with:
    * `3.0.3`
* [libgmp](https://gmplib.org/). Tested with:
    * `6.1.2`
    * `6.2.0`

#### Ubuntu
Ubuntu provides all dependencies in `universe`, starting from 18.04 onward.
```shell script
sudo apt install librdkafka-dev libavro-dev libcsv-dev libgmp-dev
```

#### CentOS
CentOS 7 provides [librdkafka](https://pkgs.org/download/librdkafka-devel), and [libgmp](https://pkgs.org/download/gmp-devel) in `Centos-Base`. [libcsv](https://pkgs.org/download/libcsv-devel) is available in `EPEL`.
```shell script
sudo yum install librdkafka-devel libcsv-devel gmp-devel
```

Unfortunately, libavro-c is not provided even in EPEL. It can be found in [Confluent repository](https://docs.confluent.io/current/installation/installing_cp/rhel-centos.html#get-the-software); however, the repository contains only latest version of the library, while the recommended one is `1.7.7`.

For this reason, `libavro-c` must be built from sources. The sources for version 1.7.7 can be [downloaded from GitHub](https://github.com/apache/avro/releases/tag/release-1.7.7). Build and installation instructions are provided with the code. Note `DCMAKE_INSTALL_PREFIX` sets the prefix, but the installation script creates various directories in the directory pointed by prefix, which do not match CentOS directories structure. The resulting binaries must be moved to the appropriate locations manually.

### Build
After meeting the dependencies, **setup the ADB / GPDB environment** and run the normal extension building pipeline:
```shell script
make
make install
```

Then, login to ADB / GPDB as a superuser and execute
```sql
CREATE EXTENSION kadb_fdw;
```

### Test
Tests *must* be conducted against a running cluster with *three* segments (e.g. `demo-cluster`). This requirement is due to the fact that offsets' values are checked, and (during tests) they currently depend on the number of segments in cluster.

There are two kinds of tests: common and Kafka tests.

*Common tests* do not make any assumptions about the test environment. They only require the extension to be installed and a GPDB cluster (see requirements for it above) running. Run these tests by
```shell script
make installcheck
```

*Kafka tests* check interaction with a running Kafka instance; they include *common tests* as well. An automated script to setup a running Kafka instance in Docker is provided, but it makes certain assumptions about the test environment; see [`kafka_test/README.md`](./kafka_test/README.md) for details. Run these tests by
```shell script
make installcheck-with-kafka
```

To clean not only the extension build files, but also the test environment (shut down the running Kafka instance), a special `Makefile` target is provided:
```shell script
make clean-with-kafka
```

## Example
```sql
-- Create a SERVER
DROP SERVER IF EXISTS ka_server;
CREATE SERVER ka_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers 'localhost:9092'
);

-- Create a FOREIGN TABLE
DROP FOREIGN TABLE IF EXISTS ka_table;
CREATE FOREIGN TABLE ka_table(field1 INT, field2 TEXT)
SERVER ka_server
OPTIONS (
    format 'avro',  -- Data serialization format
    k_topic 'my_topic',  -- Kafka topic
    k_consumer_group 'my_consumer_group',  -- Kafka consumer group
    k_seg_batch '100',  -- Limit on the number of Kafka messages retrieved by each GPDB segment
    k_timeout_ms '1000',  -- Kafka response timeout
    k_initial_offset '42'  -- Initial Kafka offset (for new or unknown partitions)
);

-- Issue a SELECT query as usual
SELECT * FROM ka_table;
```

See extra examples in [USAGE.md](./USAGE.md).


## SQL interface
`kadb_fdw` provides a user with several interfaces via SQL:
* `FOREIGN TABLE` `OPTIONS`. See [`CREATE FOREIGN TABLE`](https://gpdb.docs.pivotal.io/6-12/ref_guide/sql_commands/CREATE_FOREIGN_TABLE.html), [`ALTER FOREIGN TABLE`](https://gpdb.docs.pivotal.io/6-12/ref_guide/sql_commands/ALTER_FOREIGN_TABLE.html) documentation for details. The options supported by `kadb_fdw` are listed below
* An [offsets table](#offsets-table)
* A [set of functions](#functions)


### Offsets table
When an extension is created, a special schema `kadb` is created. It contains a table `kadb.offsets`, which stores mappings of Kafka partition to Kafka offset for every `FOREIGN TABLE` that has *ever* been created in the current database.

Each `FOREIGN TABLE` is identified by its [OID](https://www.postgresql.org/docs/9.4/datatype-oid.html). OID of a given `schema.table` (`schema` is optional when [`search_path` is properly set](https://gpdb.docs.pivotal.io/6-12/admin_guide/ddl/ddl-schema.html)) can be retrieved by the following command:
```sql
SELECT 'schema.table'::regclass::oid;
```

On `SELECT` to a `FOREIGN TABLE` with the given OID, a request is issued to `kadb.offsets`, and the messages are requested from Kafka starting at the offset retrieved from the table. For example, if the offset for some partition is set to `42`, the first message requested from this Kafka partition is a message with offset `42`.

Note [`k_seg_batch`](#k_seg_batch) option limits the number of messages retrieved by each GPDB host. As a result, there may be partitions from which no messages are retrieved by a single particular `SELECT`.

A set of partitions and their offsets can be changed by common SQL queries issued to `kadb.offsets`. In addition, a [set of functions](#functions) is provided for this purpose.

After a *successful* `SELECT` from a `FOREIGN TABLE`, offsets are updated according to the values received from Kafka, so that the offset in `kadb.offsets` is the next offset to be requested. For example, if the last message read from some partition had offset `84`, `kadb.offsets` will contain an entry with offset `85` for that partition.


### `FOREIGN TABLE` options
Both [`SERVER`](https://gpdb.docs.pivotal.io/6-10/ref_guide/sql_commands/CREATE_SERVER.html) and [`FOREIGN TABLE`](https://gpdb.docs.pivotal.io/6-10/ref_guide/sql_commands/CREATE_FOREIGN_TABLE.html) accept `OPTIONS` clause. Each option is a key-value pair, where both key and value are strings.

Options defined in `FOREIGN TABLE` and in `SERVER` are not distinguished from one another (in other words, the object where options are declared does not matter). However, `FOREIGN TABLE` options are prioritized over `SERVER` options (if there are two options with the same name).

Supported options are listed below.

#### `k_brokers`
*Required*.

A *comma-separated* list of Kafka brokers, each of which is *a `host` or a `host:port`* string.

#### `k_topic`
*Required*.

Kafka topic identifier.

#### `k_consumer_group`
*Required*.

Kafka consumer group identifier.

#### `k_seg_batch`
*Required*. *A positive integer*.

Maximum number of Kafka messages retrieved by each segment in GPDB cluster.

If a `LIMIT` query is issued, messages are still requested from Kafka in batches. As a result, offsets in the [offsets table](#Offsets-table) are set to offsets of the *last* retrieved message for each partition, even if its data is not in the query result.

#### `k_timeout_ms`
*Required*. *A non-negative integer*.

Timeout of requests to Kafka in milliseconds. Only messages available in Kafka during this time period are consumed by Kafka client (and make way to the result of `SELECT`).

A `SELECT` request may finish earlier - as soon as there are enough messages available in Kafka topic.

Note that **the actual maximum duration of a `SELECT` may be much longer**. It can be estimated as follows:
```
[duration] = [k_timeout_ms] * (2 + ceil([number of partitions] / [number of GPDB segments]))
```
This duration is most significantly impacted by partitions that do not have enough messages to be consumed: `kadb_fdw` will wait up to `k_timeout_ms` for new messages to become available in such partitions.

At some stages of execution, it is impossible to terminate a query before `k_timeout_ms` pass.

#### `format`
*Required*. *One of the pre-defined values (case-insensitive)*.

[Serialized data format](#deserialization):
* `avro`
* `csv`
* `text`

#### `k_initial_offset`
*A non-negative integer*. Default `0`.

An offset to use for partitions for which there are no entries in the [offsets table](#Offsets-table). This value is used when [`k_automatic_offsets`](#k_automatic_offsets) is set, and by offset management [functions](#functions).

#### `k_automatic_offsets`
*A boolean* (`true`, `false`). Default `true`.

Allow `kadb_fdw` to do the following:
* Immediately **before** each `SELECT` from a `FOREIGN TABLE`: 
    * Add partitions present in Kafka, and absent in the [offsets table](#offsets-table), to a set of partitions to read data from;
    * Automatically increase starting offset of any partition to the lowest (earliest) offset available in Kafka. A `NOTICE` is issued when such increase takes place.
* Immediately **after** each `SELECT` from a `FOREIGN TABLE`:  
    * Update the [offsets table](#offsets-table), *adding* partitions (by means of an `INSERT` query) that are present in Kafka, and absent in the [offsets table](#offsets-table). 

If set to `false`, an `ERROR` is raised when the *smallest* offset of an *existing* message *in Kafka* is *greater* than the offset in the [offsets table](#offsets-table) (for any partition).

Note that partitions present in Kafka, and absent in the [offsets table](#offsets-table) are not visible to the user if `CURSOR` is used: as noted above, the actual `INSERT` of new entries to the offsets table happens **after** a `SELECT`, while `CURSOR` is a query in progress.

*After* a *successful* `SELECT`, offsets in the [offsets table](#offsets-table) are modified independent of the value of this `OPTION` to reflect the number of messages read from Kafka.

#### `k_security_protocol`
*Required if Kerberos authentication is used*.

Security protocol to use to connect to Kafka. Currently, only `sasl_plaintext` is supported. This should be set *only* when Kerberos authentication is used.

#### `kerberos_keytab`
*Required if Kerberos authentication is used*.

Path to a keytab file for Kerberos authentication.

The file must be accessible by the user which runs ADB / GPDB processes, and must be present on every segment in the cluster (at the same path).

The presence of this option enables Kerberos authentication.

#### `kerberos_principal`
Default `kafkaclient`.

Kerberos principal name of the client that accesses Kafka.

#### `kerberos_service_name`
Default `kafka`. **Do not set this parameter unless necessary**.

Kerberos principal name that Kafka runs as, not including `/hostname@REALM`.

Internally, the value of this parameter is passed by `librdkafka` to [`sasl_client_new()`](https://linux.die.net/man/3/sasl_client_new) as the first parameter (`service`).

#### `kerberos_min_time_before_relogin`
*A non-negative integer*. Default `60000`.

Minimum time in milliseconds between key refresh attempts. Disable automatic key refresh by setting this option to `0`.

#### `avro_schema`
*JSON - a valid AVRO schema*.

AVRO schema to use. Incoming messages are deserialized in one of the two ways:
* If `avro_schema` option is set, the provided schema is used (incoming message must still be in [OCF](https://avro.apache.org/docs/1.8.1/spec.html#Object+Container+Files) format)
* Otherwise, a schema is extracted from incoming message in [OCF](https://avro.apache.org/docs/1.8.1/spec.html#Object+Container+Files) format

*Warning*. A user-provided schema cannot be validated. If the actual and the provided schema do not correspond, deserialization usually fails with `ERROR:  invalid memory alloc request size`. For this reason, `avro_schema` option must be used only for performance reasons, and only after careful consideration.

#### `csv_quote`
*A single character, represented by one byte in the current encoding*. Default `"`.

A character to use as a quote when parsing CSV.

#### `csv_delimeter`
*A single character, represented by one byte in the current encoding*. Default `,`.

A character to use as a field delimiter when parsing CSV.

#### `csv_null`
A string representing NULL value in CSV. By default, empty field is interpreted as `NULL`.

#### `csv_ignore_header`
*A boolean* (`true`, `false`). Default `false`.

Whether to ignore (do not parse) the first line of each message.

#### `csv_attribute_trim_whitespace`
*A boolean* (`true`, `false`). Default `true`.

Whether to trim trailing whitespace at the beginning and the end of each attribute (field) of a record.


### Functions
Several functions are provided by `kadb_fdw` to synchronize offsets in Kafka with the ones in the [offsets table](#offsets-table).

All functions are located in schema `kadb`.

Note that:
* No function provides transactional guarantees for Kafka. This means no assumptions can be made about what happens with offsets in Kafka before or after a function is called, even if the call is combined with a `SELECT` (from a `FOREIGN TABLE`) in the same SQL transaction;
* Some functions are **not atomic**. This means they do not produce a "snapshot" of all partitions at some point of time; instead, their result is obtained from each partition independently, at (slightly) different moments.

#### `kadb.commit_offsets(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

Commit offsets stored in the offsets table (for the given `FOREIGN TABLE` OID) to Kafka.

This method is **atomic**.

#### `kadb.load_partitions(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

*Result*:
* `ftoid`: equal to the provided `FOREIGN TABLE` OID
* `prt`: partition identifier
* `off`: [`k_initial_offset`](#k_initial_offset)

Load a list of partitions that exist in Kafka.

This method is **not atomic**.

#### `kadb.partitions_obtain(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

Add partitions returned by [`kadb.load_partitions(OID)`](#kadbload_partitionsoid) to the offsets table. Only new partitions are added; existing ones are left unchanged.

This method is **not atomic**.

#### `kadb.partitions_clean(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

Delete all entries from the offsets table (for the given `FOREIGN TABLE` OID) which do *not* exist in Kafka.

This method is **not atomic**.

#### `kadb.partitions_reset(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

Delete all entries from the offsets table (for the given `FOREIGN TABLE` OID), and add entries returned by [`kadb.load_partitions(OID)`](#kadbload_partitionsoid) instead.

This method is **not atomic**.

#### `kadb.load_offsets_at_timestamp(OID, BIGINT)`
*Parameters*:
1. OID of a `FOREIGN TABLE`
2. Timestamp: milliseconds since the UNIX Epoch (UTC)

*Result*:
* `ftoid`: equal to the provided `FOREIGN TABLE` OID
* `prt`: partition identifier
* `off`: result

Load the earliest offsets present in Kafka whose timestamps are greater or equal to the given timestamp (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table).

This method is **atomic**.

#### `kadb.offsets_to_timestamp(OID, BIGINT)`
*Parameters*:
1. OID of a `FOREIGN TABLE`
2. Timestamp: milliseconds since the UNIX Epoch (UTC)

Change offsets in the offsets table (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table) to the earliest offsets present in Kafka whose timestamps are greater or equal to the given timestamp.

This method is **atomic**.

#### `kadb.load_offsets_earliest(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

*Result*:
* `ftoid`: equal to the provided `FOREIGN TABLE` OID
* `prt`: partition identifier
* `off`: result

Load the earliest offsets present in Kafka (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table).

This method is **not atomic**.

#### `kadb.offsets_to_earliest(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

Change offsets in the offsets table (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table) to the earliest offsets present in Kafka.

This method is **not atomic**.

#### `kadb.load_offsets_latest(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

*Result*:
* `ftoid`: equal to the provided `FOREIGN TABLE` OID
* `prt`: partition identifier
* `off`: result

Load the latest offsets present in Kafka (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table).

This method is **not atomic**.

#### `kadb.offsets_to_latest(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`
    * To obtain OID from a table name, `'table_schema.table_name'::regclass::oid` can be used

Change offsets in the offsets table (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table) to the latest offsets present in Kafka.

As a result, `SELECT`s from the given `FOREIGN TABLE` return only messages inserted into Kafka after this function was called.

This method is **not atomic**.

#### `kadb.load_offsets_committed(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

*Result*:
* `ftoid`: equal to the provided `FOREIGN TABLE` OID
* `prt`: partition identifier
* `off`: result

Load the latest committed offsets present in Kafka (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table).

This method is **atomic**.

#### `kadb.offsets_to_committed(OID)`
*Parameters*:
1. OID of a `FOREIGN TABLE`

Change offsets in the offsets table (for the given `FOREIGN TABLE` OID, and only for partitions already present in the offsets table) to the latest committed offsets present in Kafka.

This method is **atomic**.


## Deserialization
`kadb_fdw` currently supports Kafka messages that are serialized in one of the following formats:
* [AVRO](https://avro.apache.org/docs/1.8.1/spec.html) [OCF](https://avro.apache.org/docs/1.8.1/spec.html#Object+Container+Files)
* CSV
* `text`

The deserialization method must be set explicitly by [`format`](#format) option.

No matter what format is used, only Kafka message payload is deserialized. Kafka message key is ignored.

### AVRO
`kadb_fdw` supports AVRO OCF serialization format with limitations.

Schemas must **not** contain [complex types](https://avro.apache.org/docs/1.8.1/spec.html#schema_complex). There are two exceptions:
* Unions of any supported type with type `null` are supported (except for such unions themselves; i.e. unions of "union of some_type with null" with null are not supported);
* [`fixed`](https://avro.apache.org/docs/1.8.1/spec.html#Fixed) is supported, and is treated the same way as `bytes`.

All [logical types](https://avro.apache.org/docs/1.8.1/spec.html#Logical+Types) defined by AVRO specification are supported.

The ADB / GPDB `FOREIGN TABLE` definition must match the actual AVRO schema.

Firstly, the following type mapping applies:
| AVRO type | PostgreSQL type |
| --- | --- |
| `string` | `TEXT`, `BPCHAR`, `VARCHAR` |
| `string` | Custom PostgreSQL type (e.g. `MONEY`). The conversion is the same as the one applied to user-provided textual input |
| `null` | Any PostgreSQL type in a column with non-`NULL` constraint |
| `boolean` | `BOOLEAN` |
| `int` | `INTEGER` |
| `long` | `BIGINT` |
| `float` | `REAL` |
| `double` | `DOUBLE PRECISION` |
| `bytes`, `fixed` | `BYTEA` |
| `decimal` | `NUMERIC` |
| `date` | `DATE` |
| `time-millis`, `time-micros` | `TIME` |
| `timestamp-millis` | `TIMESTAMP(N)`, where `N` is `1`, `2`, or `3` |
| `timestamp-micros` | `TIMESTAMP`, `TIMESTAMP(N)`, where `N` is `4` or greater |
| `duration` | `INTERVAL` |

Secondly, the **order** of columns must match the order of fields in AVRO schema.

#### Example
The following AVRO schemas can be processed by `kadb_fdw`:
```json
{
  "name":"doc",
  "type":"record",
  "fields":[
    {
      "name":"id",
      "type":"int"
    },
    {
      "name":"text",
      "type":[
        "string",
        "null"
      ]
    },
    {
      "name":"issued_on",
      "type":"int",
      "logicalType":"date"
    }
  ]
}
```
```json
{
  "name":"doc",
  "type":"record",
  "fields":[
    {
      "name":"d",
      "type":"int",
      "logicalType":"date"
    },
    {
      "name":"t_ms",
      "type":"int",
      "logicalType":"time-millis"
    },
    {
      "name":"t_us",
      "type":"long",
      "logicalType":"time-micros"
    },
    {
      "name":"ts_ms",
      "type":"long",
      "logicalType":"timestamp-millis"
    },
    {
      "name":"ts_us",
      "type":"long",
      "logicalType":"timestamp-micros"
    },
    {
      "name":"dur",
      "type":{
        "name":"dur_fixed",
        "type":"fixed",
        "size":12,
        "logicalType":"duration"
      }
    },
    {
      "name":"dec_1",
      "type":{
        "name":"dec_2_fixed",
        "type":"fixed",
        "size":6,
        "logicalType":"decimal"
      }
    },
    {
      "name":"dec_2",
      "type":{
        "name":"dec_2_fixed",
        "type":"bytes",
        "logicalType":"decimal",
        "precision":14,
        "scale":4
      }
    }
  ]
}
```

### CSV
`kadb_fdw` supports CSV serialization format.

The support of CSV is provided by [libcsv](https://github.com/rgamble/libcsv). As a result, conventions about CSV format are set by the mentioned library.

The specification of CSV is defined in [RFC 4180](https://tools.ietf.org/html/rfc4180). The concrete conventions used by libcsv are described in [this document](http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm#FileFormat).

`kadb_fdw`, taking into account these guidelines, uses the following rules for CSV parsing:
* Fields (attributes) are separated by a [delimeter character](#csv_delimeter)
* Rows (records) are separated by a newline character sequence
* Fields may be quoted, i.e. surrounded by a [quote character](#csv_quote)
* Fields that contain delimeter, quote, or newline character must be quoted
* Each instance of a quote character must be escaped with an immediately preceding quote character
* Empty fields are always treated as `NULL`s
* Empty lines are skipped (as if they were absent in the original CSV)
* Leading and trailing whitespace is removed from non-quoted fields, if the [corresponding option](#csv_attribute_trim_whitespace) is set

CSV values can be converted to any PostgreSQL datatype; the conversion is the same as the one applied to `psql` textual input.

### `text`
`text` is a serialization format for data represented as raw text in Kafka message.

When this format is set, `kadb_fdw` acts as follows:
* Each message is assumed to represent a single attribute (column) of a single tuple (row) of a `FOREIGN TABLE`;
* The data is parsed by PostgreSQL as user-provided textual data.

This implies `text` format **requires `FOREIGN TABLE` to contain exactly one attribute (column)**. It can be of any PostgreSQL type.

Kafka messages with empty content (of length `0`) are parsed into `NULL` values, so they can be counted.

#### Example
A definition of a `FOREIGN TABLE` using `text` format:
```sql
CREATE FOREIGN TABLE my_foreign_table_text(j JSON)
SERVER my_foreign_server
OPTIONS (
    format 'text',
    k_topic 'my_topic',
    k_consumer_group 'my_consumer_group',
    k_seg_batch '100',
    k_timeout_ms '5000'
);
```


## Implementation notes
This section contains notes on the implementation of `kadb_fdw`. Its intention is to document such behaviours, listing certain guarantees provided (and not provided).

### Concurrent `SELECT`s
`kadb_fdw` uses an [offsets table](#offsets-table) at each `SELECT` request from a `FOREIGN TABLE`. This is a single (`DISTRIBUTED REPLICATED`) ADB / GPDB table. `kadb_fdw` may issue `INSERT`, and `UPDATE` queries to the [offsets table](#offsets-table).

As a result, limitations on concurrent operations affect concurrent `SELECT`s from `kadb_fdw` `FOREIGN TABLE`s.

#### Global deadlock detector
The way concurrent transactions are processed by GPDB is affected by [GPDB global deadlock detector](https://gpdb.docs.pivotal.io/6-12/admin_guide/dml.html#topic_gdd).

When global deadlock detector is **disabled**, each `UPDATE` requires `ExclusiveLock`, which basically locks the whole table being updated. In `kadb_fdw`, this means multiple concurrent `SELECT`s (from *different* `FOREIGN TABLE`s) are not possible. Such `SELECT`s are executed sequentially, one at a time.

To allow multiple concurrent `SELECT`s (from *different* `FOREIGN TABLE`s), **enable** the global deadlock detector. With the detector enabled, each `UPDATE` requires only `RowExclusiveLock`, thus permitting multiple concurrent `UPDATE`s to the [offsets table](#offsets-table).

Concurrent `SELECT`s from a *single* `FOREIGN TABLE` are not possible. In some circumstances such concurrent `SELECT`s may succeed and produce correct results (all queries would return the same result); however, this is not guaranteed.

To enable the global deadlock detector, set [`gp_enable_global_deadlock_detector`](https://gpdb.docs.pivotal.io/6-12/ref_guide/config_params/guc-list.html#gp_enable_global_deadlock_detector) GPDB configuration variable to `on`:
```shell script
gpconfig -c gp_enable_global_deadlock_detector -v on
```

### Partition distribution
Each `SELECT` considers only partitions present in the [offsets table](#offsets-table). Its contents may be modified before a `SELECT` if [`k_automatic_offsets`](#k_automatic_offsets) is set, or by some [functions](#functions).

Partitions are distributed among *GPDB segments* according to the following rules:
1. Partitions are distributed in equal proportions among all segments. The actual number of partitions assigned to a segment varies by 1 across all segments.
2. The order of partitions (as returned from Kafka) and order of segments which they are assigned to match.

For example:
* If a cluster consists of three segments
* And Kafka returns five partitions `[0, 2, 3, 4, 1]`

The resulting partition distribution is, by segments:
1. `[0, 2]`
2. `[3, 4]`
3. `[1]`
