-- New OPTION k_automatic_offsets

ALTER FOREIGN DATA WRAPPER kadb_fdw
OPTIONS (
    DROP k_allow_offset_increase,
    ADD k_automatic_offsets 'true'
);

-- Synchronization of offsets table and Kafka partititons

CREATE FUNCTION kadb.load_partitions(
    IN oid,
    OUT ftoid oid, OUT prt INTEGER, OUT off BIGINT
)
RETURNS SETOF record
LANGUAGE C STRICT VOLATILE
AS '$libdir/kadb_fdw', 'kadb_load_partitions';

CREATE FUNCTION kadb.partitions_obtain(oid)
RETURNS void
LANGUAGE C STRICT VOLATILE
AS '$libdir/kadb_fdw', 'kadb_partitions_obtain';

CREATE FUNCTION kadb.partitions_clean(oid)
RETURNS void
LANGUAGE C STRICT VOLATILE
AS '$libdir/kadb_fdw', 'kadb_partitions_clean';

CREATE FUNCTION kadb.partitions_reset(oid)
RETURNS void
LANGUAGE C STRICT VOLATILE
AS '$libdir/kadb_fdw', 'kadb_partitions_reset';

-- Offsets at timestamp

CREATE FUNCTION kadb.load_offsets_at_timestamp(
    IN oid, IN BIGINT,
    OUT ftoid oid, OUT prt INTEGER, OUT off BIGINT
)
RETURNS SETOF record
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_load_offsets_at_timestamp'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION kadb.offsets_to_timestamp(oid, BIGINT)
RETURNS void
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_offsets_to_timestamp'
LANGUAGE C STRICT VOLATILE;

-- Earliest offsets

CREATE FUNCTION kadb.load_offsets_earliest(
    IN oid,
    OUT ftoid oid, OUT prt INTEGER, OUT off BIGINT
)
RETURNS SETOF record
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_load_offsets_earliest'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION kadb.offsets_to_earliest(oid)
RETURNS void
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_offsets_to_earliest'
LANGUAGE C STRICT VOLATILE;

-- Latest offsets

CREATE FUNCTION kadb.load_offsets_latest(
    IN oid,
    OUT ftoid oid, OUT prt INTEGER, OUT off BIGINT
)
RETURNS SETOF record
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_load_offsets_latest'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION kadb.offsets_to_latest(oid)
RETURNS void
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_offsets_to_latest'
LANGUAGE C STRICT VOLATILE;

-- Committed offsets

CREATE FUNCTION kadb.load_offsets_committed(
    IN oid,
    OUT ftoid oid, OUT prt INTEGER, OUT off BIGINT
)
RETURNS SETOF record
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_load_offsets_committed'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION kadb.offsets_to_committed(oid)
RETURNS void
EXECUTE ON MASTER
AS '$libdir/kadb_fdw', 'kadb_offsets_to_committed'
LANGUAGE C STRICT VOLATILE;
