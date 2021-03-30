#ifndef KADB_FDW_OFFSETS_INCLUDED
#define KADB_FDW_OFFSETS_INCLUDED

/*
 * Offsets storage for Kafka-ADB.
 *
 * The main component of offsets storage is two tables, called "distributed" and
 * "global".
 *
 * The global table stores offsets persistently. It is a 'DISTRIBUTED
 * REPLICATED' table created once when the FDW extension is installed.
 *
 * The local table is created at 'ForeignScan' start and is dropped at its end.
 * This is a 'DISTRIBUTED RANDOMLY' table used to collect updated offsets from
 * GPDB segments.
 */

#include <postgres.h>

#include <nodes/pg_list.h>


typedef struct PartitionOffsetPair
{
	int32_t		partition;
	int64_t		offset;
}	PartitionOffsetPair;


/**
 * Create a temporary distributed offsets' table for a FOREIGN TABLE with the
 * given 'ftoid'. The created table is DROPped at transaction end.
 *
 * The presence of this table (one per session) also makes sure two CURSORs
 * cannot exist in one session for the same FOREIGN TABLE.
 *
 * @return OID of the created distributed table
 */
Oid			create_distributed_table(Oid ftoid);

/**
 * Drop a temporary distributed offsets' table, created for a FOREIGN TABLE with
 * the given 'ftoid'.
 */
void		drop_distributed_table(Oid ftoid);

/**
 * Write offsets to a distributed table.
 *
 * @param dtoid OID of the distributed table to write offsets to
 * @param partition_offset_pairs a list of 'PartitionOffsetPair's
 */
void		write_distributed_offsets(Oid dtoid, List *partition_offset_pairs);

/**
 * Update the global offsets' table with new offsets from a distributed offsets'
 * table. Insert new pairs, if necessary, identified by 'absent_partitions'.
 * 
 * We could conduct SELECT from the global offsets table to check which
 * partitions are absent (and thus do not require the second parameter).
 * However, this method is called by ExecEndForeignScan(), and it is impossible
 * to execute the necessary query due to "could not serialize current snapshot, 
 * ActiveSnapshot not set" error.
 * 
 * @param absent_partitions a list of Int: partitions not present in the global
 * offsets' table
 */
void		update_partition_offset_pairs_with_distributed_offsets(Oid ftoid, List *partitions_absent);

/**
 * Load partition-offset pairs from the global offsets' table.
 *
 * @param ftoid OID of the foreign table
 * @param partitions a list of Int - partition identifiers. Used only for
 * filtering: 'list_length(partitions)' is NOT GUARANTEED to match the
 * 'list_length()' of the return value. If NIL, partition-offset pairs for all
 * (present) partitions are returned.
 *
 * @return a list of 'PartitionOffsetPair *'s
 */
List	   *load_partition_offset_pairs(Oid ftoid, List *partitions);

/**
 * Check whether the given 'partitions' are present in the global offsets'
 * table, and load their offsets. New partition-offset pairs are put into
 * 'partition_offset_pairs_absent' with offset set to 'initial_offset'; existing
 * ones are put into 'partition_offset_pairs_present'.
 *
 * @param ftoid OID of the foreign table
 * @param initial_offset offset to set for absent partition-offset pairs. If <0,
 * offset for such pairs is not defined.
 * @param partitions a list of Int - partition identifiers
 * @param partition_offset_pairs_present a pointer to the resulting list:
 * partition-offset pairs present in the global offsets' table. May be NULL
 * @param partition_offset_pairs_absent a pointer to the resulting list:
 * partition-offset pairs absent in the global offsets' table. May be NULL
 *
 * @note when this method defines (sets) an offset of a partition-offset pair,
 * this is reported by a NOTICE.
 */
void		check_partition_offset_pairs_presence(Oid ftoid, int64_t initial_offset, List *partitions, List **partition_offset_pairs_present, List **partition_offset_pairs_absent);

/**
 * INSERT 'partition_offset_pairs' into the global offsets' table for the given
 * 'ftoid'.
 *
 * @param partition_offset_pairs a list of 'PartitionOffsetPair *'
 */
void		add_partition_offset_pairs(Oid ftoid, List *partition_offset_pairs);

/**
 * INSERT 'partitions', converted to partition-offset pairs into the global
 * offsets' table for the given 'ftoid'.
 *
 * This method calls 'add_partition_offset_pairs', but accepts a list of
 * partitions, not pairs.
 *
 * @param partitions a list of Int
 * @param initial_offset offset set for each inserted partition-offset pair
 */
void		add_partitions(Oid ftoid, List *partitions, int64_t initial_offset);

/**
 * UPDATE the provided 'partition_offset_pairs' in the global offsets' table,
 * setting their offsets to values given in 'partition_offset_pairs' list.
 *
 * @note All partitions from the given 'partition_offset_pairs' must already
 * exist in the global offsets' table. Missing partitions are not updated, and
 * failure to do so is not reported.
 */
void		update_partition_offset_pairs(Oid ftoid, List *partition_offset_pairs);

/**
 * DELETE entries in the global offsets' table for the given 'ftoid'.
 *
 * @param partitions a list of Int. Used for filtering: if not NIL, only entries
 * from this list are deleted.
 */
void		delete_partition_offset_pairs(Oid ftoid, List *partitions);

/**
 * Find the difference between two lists of 'PartitionOffsetPair *': 'a' / 'b'.
 *
 * @return a list of 'PartitionOffsetPair *' (shallow-copied from 'a') which
 * are present in 'a', but absent in 'b'.
 *
 * @note only partitions (not offsets) are taken into account.
 */
List	   *partition_offset_pairs_difference(List *a, List *b);


#endif   /* KADB_FDW_OFFSETS_INCLUDED */
