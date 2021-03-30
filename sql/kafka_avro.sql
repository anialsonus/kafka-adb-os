-- Test how extension deserializes some AVRO types
-- This test REQUIRES a running Kafka instance. See 'kafka_test' directory for
-- instructions on how to setup one

-- start_matchignore
-- m/^NOTICE:  Kafka-ADB: Offset for partition [0-9]* is not known, and is set to default value [0-9]*$/
-- end_matchignore

-- start_ignore
DROP EXTENSION IF EXISTS kadb_fdw CASCADE;
CREATE EXTENSION kadb_fdw;

CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers 'localhost:9092',
    format 'avro'
);
-- end_ignore


-- Test: Exact types

-- start_ignore
DROP FOREIGN TABLE IF EXISTS test_kadb_fdw_table;

CREATE FOREIGN TABLE test_kadb_fdw_table(
    d DATE,
    t_ms TIME,
    t_us TIME,
    ts_ms TIMESTAMP(3),
    ts_us TIMESTAMP(6),
    dur INTERVAL,
    dec_1 NUMERIC,
    dec_2 NUMERIC(14, 4),
    n TEXT NULL,
    b_bytes BYTEA,
    b_fixed BYTEA,
    f REAL,
    dbl DOUBLE PRECISION,
    bln BOOLEAN,
    postgres_type MONEY
)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'kadb_fdw_test_avro',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '5',
    k_timeout_ms '2000'
);
-- end_ignore

-- start_ignore
SET client_min_messages = WARNING;
SET extra_float_digits = 3;
-- end_ignore

SELECT
    to_char(d, 'YYYY-MM-DD') AS d,
    to_char(t_ms, 'HH24:MI:SS.MS') AS t_ms,
    to_char(t_us, 'HH24:MI:SS.US') AS t_us,
    to_char(ts_ms, 'YYYY-MM-DD HH24:MI:SS.MS') AS ts_ms,
    (EXTRACT(EPOCH FROM ts_ms) * 1000000)::BIGINT AS ts_ms_epoch,
    to_char(ts_us, 'YYYY-MM-DD HH24:MI:SS.US') AS ts_us,
    (EXTRACT(EPOCH FROM ts_us) * 1000000)::BIGINT AS ts_us_epoch,
    to_char(dur, 'YYYY-MM-DD HH24:MI:SS.MS') AS dur,
    dec_1,
    dec_2,
    (n IS NULL) AS n,
    b_bytes,
    b_fixed,
    f,
    dbl,
    bln,
    postgres_type::NUMERIC
FROM test_kadb_fdw_table
ORDER BY ts_us_epoch;

-- start_ignore
RESET extra_float_digits;
RESET client_min_messages;
-- end_ignore


-- Test: Default timestamp precision

-- start_ignore
DROP FOREIGN TABLE IF EXISTS test_kadb_fdw_table;

CREATE FOREIGN TABLE test_kadb_fdw_table(
    d DATE,
    t_ms TIME,
    t_us TIME,
    ts_ms TIMESTAMP(3),
    ts_us TIMESTAMP,
    dur INTERVAL,
    dec_1 NUMERIC,
    dec_2 NUMERIC(14, 4),
    n TEXT NULL,
    b_bytes BYTEA,
    b_fixed BYTEA,
    f REAL,
    dbl DOUBLE PRECISION,
    bln BOOLEAN,
    postgres_type MONEY
)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'kadb_fdw_test_avro',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '5',
    k_timeout_ms '2000'
);
-- end_ignore

-- start_ignore
SET client_min_messages = WARNING;
-- end_ignore

SELECT
    to_char(ts_us, 'YYYY-MM-DD HH24:MI:SS.US') AS ts_us,
    (EXTRACT(EPOCH FROM ts_us) * 1000000)::BIGINT AS ts_us_epoch
FROM test_kadb_fdw_table
ORDER BY ts_us_epoch;

-- start_ignore
RESET client_min_messages;
-- end_ignore
