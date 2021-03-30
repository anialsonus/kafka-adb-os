\echo use "alter extension kadb_fdw update to '0.6'" to load this file. \quit

SET client_min_messages = ERROR;

CREATE FUNCTION kadb.fdw_validator(text[], oid)
RETURNS void
AS '$libdir/kadb_fdw', 'kadb_fdw_validator'
LANGUAGE C STRICT;

ALTER FOREIGN DATA WRAPPER kadb_fdw
VALIDATOR kadb.fdw_validator;

RESET client_min_messages;
