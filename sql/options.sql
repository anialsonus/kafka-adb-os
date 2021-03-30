-- Test the extension validates OPTIONs properly
-- These tests do not require a Kafka instance. They can be run as common tests.

-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

DROP EXTENSION IF EXISTS kadb_fdw CASCADE;
CREATE EXTENSION kadb_fdw;

SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: Single unknown OPTION in SERVER

CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro',
    unknown_option 'unknown_value'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server;
-- end_ignore


-- Test: Multiple unknown OPTIONs in SERVER

CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    unknown_option_2 'unknown_value_2',
    k_brokers '0.0.0.0:9092',
    format 'avro',
    unknown_option 'unknown_value'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server;
-- end_ignore


-- Test: Unknown OPTIONs do not transit to TABLE

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro',
    unknown_option 'unknown_value'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: Single unknown OPTION in TABLE

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40',
    unknown_option 'unknown_value'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: Multiple unknown OPTIONs in TABLE

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    unknown_option_2 'unknown_value_2',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40',
    unknown_option 'unknown_value'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: SELECT issues WARNINGs about unknown OPTION both in SERVER and in TABLE

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro',
    unknown_option_server 'unknown_value_server'
);

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    unknown_option_table 'unknown_value_table',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_none', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_table;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: Invalid OPTION in SERVER

CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro',
    k_automatic_offsets 'unknown_value'
);

-- start_ignore
DROP SERVER IF EXISTS test_kadb_fdw_server;
-- end_ignore


-- Test: Invalid OPTION in TABLE

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_automatic_offsets 'unknown_value',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: Invalid boolean OPTION

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40',
    format 'csv',
    csv_ignore_header 'unknown_value'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: Empty int OPTION

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    format 'avro',
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '',
    k_timeout_ms '1000'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore


-- Test: Historical invalid OPTION in SERVER

CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro',
    k_allow_offset_increase 'unknown_value'
);

-- start_ignore
DROP SERVER IF EXISTS test_kadb_fdw_server;
-- end_ignore


-- Test: Historical invalid OPTION in TABLE

-- start_ignore
CREATE SERVER test_kadb_fdw_server
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers '0.0.0.0:9092',
    format 'avro'
);
-- end_ignore

CREATE FOREIGN TABLE test_kadb_fdw_table(i INT, t TEXT)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_allow_offset_increase 'unknown_value',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40'
);

-- start_ignore
DROP SERVER test_kadb_fdw_server CASCADE;
-- end_ignore
