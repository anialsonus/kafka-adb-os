#ifndef KADB_FDW_FUNCTIONS_AUXILIARY_INCLUDED
#define KADB_FDW_FUNCTIONS_AUXILIARY_INCLUDED

/*
 * Auxiliary user-space functions for Kafka-ADB.
 */

#include <postgres.h>

#include <fmgr.h>


/**
 * PostgreSQL wrapper around 'ft_commit_offsets'
 */
Datum		kadb_commit_offsets(PG_FUNCTION_ARGS);

/**
 * PostgreSQL wrapper around 'ft_load_offsets_at_timestamp'
 */
Datum		kadb_load_offsets_at_timestamp(PG_FUNCTION_ARGS);

/**
 * PostgreSQL wrapper around 'ft_load_offsets_earliest'
 */
Datum		kadb_load_offsets_earliest(PG_FUNCTION_ARGS);

/**
 * PostgreSQL wrapper around 'ft_load_offsets_latest'
 */
Datum		kadb_load_offsets_latest(PG_FUNCTION_ARGS);

/**
 * PostgreSQL wrapper around 'ft_load_offsets_committed'
 */
Datum		kadb_load_offsets_committed(PG_FUNCTION_ARGS);

/**
 * PostgreSQL wrapper around 'ft_load_partitions'
 */
Datum		kadb_load_partitions(PG_FUNCTION_ARGS);


#endif   /* KADB_FDW_FUNCTIONS_AUXILIARY_INCLUDED */
