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


-- Test: kadb.load_partitions()

SELECT prt, off FROM kadb.load_partitions('test_kadb_fdw_table'::regclass::oid)
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

SELECT prt, off FROM kadb.load_partitions('test_kadb_fdw_table'::regclass::oid)
WHERE ftoid <> 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: kadb.partitions_reset()

SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);
-- end_ignore

SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: kadb.partitions_clean()

SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

-- start_ignore
INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);
-- end_ignore

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

SELECT kadb.partitions_clean('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: kadb.partitions_obtain()

-- start_ignore
DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;

INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);
-- end_ignore

SELECT kadb.partitions_obtain('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: Offsets at timestamp

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT prt, off FROM kadb.load_offsets_at_timestamp('test_kadb_fdw_table'::regclass::oid, 1000000000000)
ORDER BY prt;

SELECT kadb.offsets_to_timestamp('test_kadb_fdw_table'::regclass::oid, 1000000000000);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_timestamp('test_kadb_fdw_table'::regclass::oid, 1000000000000);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid AND prt = 0;

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_timestamp('test_kadb_fdw_table'::regclass::oid, 1000000000000);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_timestamp('test_kadb_fdw_table'::regclass::oid, 1000000000000);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: Earliest offsets

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT prt, off FROM kadb.load_offsets_earliest('test_kadb_fdw_table'::regclass::oid)
ORDER BY prt;

SELECT kadb.offsets_to_earliest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_earliest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid AND prt = 0;

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_earliest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_earliest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: Latest offsets

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT prt, off FROM kadb.load_offsets_latest('test_kadb_fdw_table'::regclass::oid)
ORDER BY prt;

SELECT kadb.offsets_to_latest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_latest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid AND prt = 0;

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_latest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_latest('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;


-- Test: Committed offsets

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT prt, off FROM kadb.load_offsets_committed('test_kadb_fdw_table'::regclass::oid)
ORDER BY prt;

SELECT kadb.offsets_to_committed('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_committed('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

DELETE FROM kadb.offsets
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid AND prt = 0;

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_committed('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;

-- start_ignore
SELECT kadb.partitions_reset('test_kadb_fdw_table'::regclass::oid);

INSERT INTO kadb.offsets(ftoid, prt, off) VALUES
('test_kadb_fdw_table'::regclass::oid, 42, 42);

UPDATE kadb.offsets SET off = 1 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid;
-- end_ignore

SELECT kadb.offsets_to_committed('test_kadb_fdw_table'::regclass::oid);

SELECT prt, off FROM kadb.offsets 
WHERE ftoid = 'test_kadb_fdw_table'::regclass::oid
ORDER BY prt;
