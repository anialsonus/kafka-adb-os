-- Test how extension interacts with a working Kafka instance
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


-- Test: Multiple partitions, single huge SELECT

-- start_ignore
DROP FOREIGN TABLE IF EXISTS test_kadb_fdw_table;

CREATE FOREIGN TABLE test_kadb_fdw_table(
    id INT,
    gnr TEXT,
    stu TEXT,
    ser TEXT,
    num TEXT,
    iss_by TEXT,
    iss_da INT,
    iss_plc TEXT,
    exp_dat INT,
    det_dat TEXT,
    r_obj INT,
    crt_on INT,
    upd_on INT,
    dsc TEXT NULL,
    iss_id TEXT,
    vrf_stu TEXT,
    vrf_on INT,
    sys_op INT
)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'kadb_fdw_test',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '5',
    k_timeout_ms '2000'
);
-- end_ignore

-- start_ignore
SET client_min_messages = WARNING;
-- end_ignore

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;

-- start_ignore
RESET client_min_messages;
-- end_ignore


-- Test: Multiple partitions, multiple small SELECTs in a single transaction

-- start_ignore
DROP FOREIGN TABLE IF EXISTS test_kadb_fdw_table;

CREATE FOREIGN TABLE test_kadb_fdw_table(
    id INT,
    gnr TEXT,
    stu TEXT,
    ser TEXT,
    num TEXT,
    iss_by TEXT,
    iss_da INT,
    iss_plc TEXT,
    exp_dat INT,
    det_dat TEXT,
    r_obj INT,
    crt_on INT,
    upd_on INT,
    dsc TEXT NULL,
    iss_id TEXT,
    vrf_stu TEXT,
    vrf_on INT,
    sys_op INT
)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'kadb_fdw_test',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '2',
    k_timeout_ms '2000'
);
-- end_ignore

BEGIN;
-- start_ignore
CREATE TEMPORARY TABLE select_results(id INT, gnr TEXT, dsc TEXT NULL) ON COMMIT DROP DISTRIBUTED RANDOMLY;

INSERT INTO pg_temp.select_results SELECT id, gnr, dsc FROM test_kadb_fdw_table;
INSERT INTO pg_temp.select_results SELECT id, gnr, dsc FROM test_kadb_fdw_table;
INSERT INTO pg_temp.select_results SELECT id, gnr, dsc FROM test_kadb_fdw_table;
-- end_ignore

SELECT id, gnr, dsc FROM pg_temp.select_results ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;
COMMIT;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;


-- Test: A topic with a single partition

-- start_ignore
DROP FOREIGN TABLE IF EXISTS test_kadb_fdw_table;

CREATE FOREIGN TABLE test_kadb_fdw_table(
    id INT,
    gnr TEXT,
    stu TEXT,
    ser TEXT,
    num TEXT,
    iss_by TEXT,
    iss_da INT,
    iss_plc TEXT,
    exp_dat INT,
    det_dat TEXT,
    r_obj INT,
    crt_on INT,
    upd_on INT,
    dsc TEXT NULL,
    iss_id TEXT,
    vrf_stu TEXT,
    vrf_on INT,
    sys_op INT
)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'kadb_fdw_test_single_partition',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '3',
    k_timeout_ms '2000'
);
-- end_ignore

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;


-- Test: An empty topic

-- start_ignore
DROP FOREIGN TABLE IF EXISTS test_kadb_fdw_table;

CREATE FOREIGN TABLE test_kadb_fdw_table(
    id INT,
    gnr TEXT,
    stu TEXT,
    ser TEXT,
    num TEXT,
    iss_by TEXT,
    iss_da INT,
    iss_plc TEXT,
    exp_dat INT,
    det_dat TEXT,
    r_obj INT,
    crt_on INT,
    upd_on INT,
    dsc TEXT NULL,
    iss_id TEXT,
    vrf_stu TEXT,
    vrf_on INT,
    sys_op INT
)
SERVER test_kadb_fdw_server
OPTIONS (
    k_topic 'kadb_fdw_test_empty',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '1',
    k_timeout_ms '2000'
);
-- end_ignore

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;


-- Test: A server that is not available

-- start_ignore
CREATE SERVER test_kadb_fdw_server_not_exists
FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    k_brokers 'localhost:0',
    format 'avro'
);

CREATE FOREIGN TABLE test_kadb_fdw_table_server_not_exists(
    i INT
)
SERVER test_kadb_fdw_server_not_exists
OPTIONS (
    k_topic 'kadb_fdw_test',
    k_consumer_group 'test_consumer_group',
    k_seg_batch '2',
    k_timeout_ms '2000'
);
-- end_ignore

SELECT i FROM test_kadb_fdw_table_server_not_exists;
