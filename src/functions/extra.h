#ifndef KADB_FDW_FUNCTIONS_EXTRA_INCLUDED
#define KADB_FDW_FUNCTIONS_EXTRA_INCLUDED

/*
 * Extra auxiliary user-space functions for Kafka-ADB.
 *
 * These functions may be replaced by pure SQL involving (non-extra) auxiliary
 * functions.
 */

#include <postgres.h>

#include <fmgr.h>


/**
 * Add new (unknown) partitions to the offsets' table, but do not change its
 * existing contents.
 */
Datum		kadb_partitions_obtain(PG_FUNCTION_ARGS);

/**
 * Remove partitions, that do not exist in Kafka, from the offsets' table.
 */
Datum		kadb_partitions_clean(PG_FUNCTION_ARGS);

/**
 * Reset all partitions in the offsets' table, and fill it with partitions
 * present in Kafka at the moment, with default (initial) offset set.
 */
Datum		kadb_partitions_reset(PG_FUNCTION_ARGS);

/**
 * 'ft_load_offsets_at_timestamp()' with built-in UPDATE of the offsets' table.
 */
Datum		kadb_offsets_to_timestamp(PG_FUNCTION_ARGS);

/**
 * 'ft_load_offsets_earliest()' with built-in UPDATE of the offsets' table.
 */
Datum		kadb_offsets_to_earliest(PG_FUNCTION_ARGS);

/**
 * 'ft_load_offsets_latest()' with built-in UPDATE of the offsets' table.
 */
Datum		kadb_offsets_to_latest(PG_FUNCTION_ARGS);

/**
 * 'ft_load_offsets_committed()' with built-in UPDATE of the offsets' table.
 */
Datum		kadb_offsets_to_committed(PG_FUNCTION_ARGS);


#endif   /* KADB_FDW_FUNCTIONS_EXTRA_INCLUDED */
