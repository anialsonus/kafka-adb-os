-- Test 'text' deserialization
-- These tests do not require a Kafka instance. They can be run as common tests.

-- start_matchignore
-- m/^NOTICE:  Kafka-ADB: Offset for partition [0-9]* is not known, and is set to default value [0-9]*.*/
-- end_matchignore

-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

DROP EXTENSION IF EXISTS kadb_fdw CASCADE;
CREATE EXTENSION kadb_fdw;

CREATE SERVER test_kadb_fdw_distribution_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092'
);
-- end_ignore


-- Test: CREATE TABLE with format 'text'

CREATE FOREIGN TABLE test_kadb_fdw_text(j JSON)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject '{"a": "Letter A", "b": {"b1": 1, "b2": "Field B2"}, "c": 3, "d": null}'
);

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: ERROR for a SELECT from a table with more than one column

-- start_ignore
CREATE FOREIGN TABLE test_kadb_fdw_text(a TEXT, b JSON, c INT, d TEXT)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject '{"a": "Letter A", "b": {"b1": 1, "b2": "Field B2"}, "c": 3, "d": null}'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_text', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT a, b, c, d FROM test_kadb_fdw_text;

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: ERROR for a SELECT from a table with one normal and one dropped column

-- start_ignore
CREATE FOREIGN TABLE test_kadb_fdw_text(a TEXT, b JSON)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject '{"a": "Letter A", "b": {"b1": 1, "b2": "Field B2"}, "c": 3, "d": null}'
);

ALTER TABLE test_kadb_fdw_text DROP COLUMN a;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_text', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT b FROM test_kadb_fdw_text;

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: ERROR for a SELECT from a table with a single dropped column

-- start_ignore
CREATE FOREIGN TABLE test_kadb_fdw_text(j JSON)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject '{"a": "Letter A", "b": {"b1": 1, "b2": "Field B2"}, "c": 3, "d": null}'
);

ALTER TABLE test_kadb_fdw_text DROP COLUMN j;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_text', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT * FROM test_kadb_fdw_text;

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: Normal SELECT, JSON data

-- start_ignore

CREATE FOREIGN TABLE test_kadb_fdw_text(j JSON)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject '{"a": "Letter A", "b": {"b1": 1, "b2": "Field B2"}, "c": 3, "d": null}'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_text', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT j FROM test_kadb_fdw_text;

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: Normal SELECT, INT data

-- start_ignore
CREATE FOREIGN TABLE test_kadb_fdw_text(i INT)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject '42'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_text', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i FROM test_kadb_fdw_text;

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: Normal SELECT, empty data

-- start_ignore
CREATE FOREIGN TABLE test_kadb_fdw_text(i INT)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    format 'text',

    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',

    k_tuples_per_partition_on_inject '1',
    text_data_on_inject ''
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_text', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i FROM test_kadb_fdw_text;

-- start_ignore
DROP FOREIGN TABLE test_kadb_fdw_text;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore
