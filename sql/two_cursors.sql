-- Test the extension does not allow to create two cursors in a single session
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
    k_brokers '0.0.0.0:9092',
    format 'avro'
);
-- end_ignore


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
    k_tuples_per_partition_on_inject '2',
    k_initial_offset '40'
);
-- end_ignore


-- Test: Two cursors in a single session are forbidden

-- start_ignore
SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

BEGIN;
DECLARE curs CURSOR FOR SELECT i, t FROM test_kadb_fdw_t;
FETCH 1 FROM curs;
DECLARE curs2 CURSOR FOR SELECT i, t FROM test_kadb_fdw_t;
ROLLBACK;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT prt, off
FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_t'::regclass::oid
ORDER BY prt;


-- Test: Two cursors in a single session are forbidden (no FETCH)

-- start_ignore
SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

BEGIN;
DECLARE curs CURSOR FOR SELECT i, t FROM test_kadb_fdw_t;
DECLARE curs2 CURSOR FOR SELECT i, t FROM test_kadb_fdw_t;
ROLLBACK;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT prt, off
FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_t'::regclass::oid
ORDER BY prt;


-- Test: SELECT is not allowed when CURSOR is in use

-- start_ignore
SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_2_per_segment', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

BEGIN;
DECLARE curs CURSOR FOR SELECT i, t FROM test_kadb_fdw_t;
FETCH 1 FROM curs;
SELECT i, t FROM test_kadb_fdw_t;
ROLLBACK;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT prt, off
FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_t'::regclass::oid
ORDER BY prt;
