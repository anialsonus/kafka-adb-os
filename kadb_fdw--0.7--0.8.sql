CREATE FUNCTION kadb.commit_offsets(oid)
RETURNS void
AS '$libdir/kadb_fdw', 'kadb_commit_offsets'
LANGUAGE C STRICT;
