\echo use "create extension kadb_fdw" to load this file. \quit

CREATE TABLE kadb.offsets (
    ftoid OID,
    prt INTEGER NOT NULL,
    off BIGINT NOT NULL,
    PRIMARY KEY(ftoid, prt)
)
DISTRIBUTED REPLICATED;

CREATE FUNCTION kadb.fdw_handler()
RETURNS fdw_handler
AS '$libdir/kadb_fdw', 'kadb_fdw_handler'
LANGUAGE C STRICT
SECURITY DEFINER;

CREATE FOREIGN DATA WRAPPER kadb_fdw
HANDLER kadb.fdw_handler
OPTIONS (
    mpp_execute 'all segments',
    k_initial_offset '0',
    k_allow_offset_increase 'true'
);
