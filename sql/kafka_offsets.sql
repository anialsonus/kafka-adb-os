-- Test how extension synchronizes offsets with Kafka
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


-- Test: Automatic partition setup

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

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;


-- Test: kadb.commit_offsets()

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

SELECT id, gnr, dsc FROM test_kadb_fdw_table ORDER BY id, gnr, dsc;
-- end_ignore

SELECT prt, off FROM kadb.offsets WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid ORDER BY prt;

SELECT kadb.commit_offsets('test_kadb_fdw_table'::regclass::oid);

-- kafka-consumer-groups succeeds only when offsets are committed.
-- Otherwise, it fails: "Consumer group 'test_consumer_group' does not exist."
\! docker exec broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group test_consumer_group 2>/dev/null | sort
