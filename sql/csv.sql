-- Test CSV deserialization
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
    k_initial_offset '40',

    format 'csv',

    k_tuples_per_partition_on_inject '1',
    csv_data_on_inject ''
);
-- end_ignore


-- Test: Single-line CSV, without newline at end

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (SET csv_data_on_inject
'1,one'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: Three-lines CSV, without newline at end

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (SET csv_data_on_inject
'1,one
2,two
3,three'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: CSV with less fields than the FOREIGN TABLE has

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (SET csv_data_on_inject
'1
2
3
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;
SELECT i FROM test_kadb_fdw_t WHERE t IS NULL ORDER BY i;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: CSV with varying number of fields

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (SET csv_data_on_inject
'1
2,two
3
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;
SELECT i FROM test_kadb_fdw_t WHERE t IS NULL ORDER BY i;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: CSV with quotes

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (SET csv_data_on_inject
'1,"one, first"
"2",two
3,three
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: CSV with quoted (escaped) quotes

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (SET csv_data_on_inject
'1,"one ""first"""
2,"quote""s"
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: CSV with custom quotes, without newline at end

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    ADD csv_quote '#',
    SET csv_data_on_inject
'1,#"one first"#
2,#two ## hashes#
3,#three, third#'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    DROP csv_quote
);
-- end_ignore


-- Test: CSV with custom delimiter

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    ADD csv_delimeter '#',
    SET csv_data_on_inject
'1#one
2#two, second
3#"three #third"
4#
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    DROP csv_delimeter
);
-- end_ignore


-- Test: CSV with custom null string

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    ADD csv_null '<null>',
    SET csv_data_on_inject
'1,one
2,<null>
3,
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;
SELECT i FROM test_kadb_fdw_t WHERE t IS NULL ORDER BY i;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    DROP csv_null
);
-- end_ignore


-- Test: CSV with trailing whitespace

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    SET csv_data_on_inject
'1, one
2 ,two
 3 , three 
4, "four fourth"
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;
-- end_ignore


-- Test: CSV with trailing whitespace and disabled trimming

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    ADD csv_attribute_trim_whitespace 'false',
    SET csv_data_on_inject
'1,one
2 ,two
 3 , three 
4, "four fourth"
'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    DROP csv_attribute_trim_whitespace
);
-- end_ignore


-- Test: CSV with ignored header

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    ADD csv_ignore_header 'true',
    SET csv_data_on_inject
'number,number name
1,one
2,two'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    DROP csv_ignore_header
);
-- end_ignore


-- Test: CSV with NOT ignored header

-- start_ignore
ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    ADD csv_ignore_header 'false',
    SET csv_data_on_inject
'1,one
2,two'
);

SELECT gp_inject_fault_infinite('kadb_fdw_inject_tuples', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_partition_1_per_segment', 'skip', dbid)
FROM gp_segment_configuration;

SELECT gp_inject_fault_infinite('kadb_fdw_inject_csv', 'skip', dbid)
FROM gp_segment_configuration;
-- end_ignore

SELECT i, t FROM test_kadb_fdw_t ORDER BY i, t;

-- start_ignore
SELECT gp_inject_fault('all', 'reset', dbid)
FROM gp_segment_configuration;

ALTER FOREIGN TABLE test_kadb_fdw_t OPTIONS (
    DROP csv_ignore_header
);
-- end_ignore
