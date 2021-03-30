-- Various tests that do not seem worth grouping
-- These tests do not require a Kafka instance. They can be run as common tests.

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


-- Test: 'format' is required

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

CREATE FOREIGN TABLE test_kadb_fdw_t(i INT, t TEXT)
SERVER test_kadb_fdw_distribution_server
OPTIONS (
    k_topic 'test_topic',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '1000',
    k_tuples_per_partition_on_inject '2'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore
