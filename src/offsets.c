#include "offsets.h"

#include <inttypes.h>

#include <access/heapam.h>
#include <access/xact.h>
#include <cdb/cdbvars.h>
#include <executor/spi.h>
#include <nodes/makefuncs.h>
#include <tcop/tcopprot.h>
#include <utils/memutils.h>

#include "utils/kadb_gp_utils.h"
#include "utils/kadb_assert.h"


/* The namespace of tables of FDW extension */
#define EXTENSION_NAMESPACE "kadb"
/* Global table name */
#define GLOBAL_TABLE_NAME "offsets"

/* Global table column: FDW table OID */
#define FTOID_COLUMN "ftoid"
/* Global & local table column: Partition ID */
#define PARTITION_COLUMN "prt"
/* Global & local table column: Offset for the corresponding partition */
#define OFFSET_COLUMN "off"


/**
 * Get the distributed offsets' table name for the given 'ftoid'.
 */
static inline char *
distributed_table_name(Oid ftoid)
{
	char	   *name = palloc(NAMEDATALEN);

	snprintf(name, NAMEDATALEN, "_kadb_offsets_%u", ftoid);
	return name;
}

/**
 * Get the "pg_temp" namespace name for the current session.
 */
static inline char *
temporary_namespace_name(void)
{
	char	   *name = palloc(NAMEDATALEN);

	snprintf(name, NAMEDATALEN, "pg_temp_%d", gp_session_id);
	return name;
}

/**
 * A wrapper around 'SPI_execute()' which fails with 'ereport(ERROR)' if an
 * error happens.
 *
 * @note Unexpected errors may happen if 'read_only' is set. In addition,
 * 'tcount' imposes a hard limit on the number of tuples processed. Use these
 * parameters cautiously.
 */
static inline void
execute_spi_or_error(const char *query, bool read_only, int64 tcount)
{
	const char *old_debug_query_string = debug_query_string;

	debug_query_string = query;

	int			r;

	PG_TRY();
	{
		r = SPI_execute(query, read_only, tcount);
	}
	PG_CATCH();
	{
		debug_query_string = old_debug_query_string;
		PG_RE_THROW();
	}
	PG_END_TRY();
	debug_query_string = old_debug_query_string;

	if (r < 0)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to execute '%s' via SPI: %s [%d]", query, SPI_result_code_string(r), r)));
}

Oid
create_distributed_table(Oid ftoid)
{
	ASSERT_CONTROLLER();

	/* Create table using SPI */

	StringInfoData query_create;

	initStringInfo(&query_create);
	appendStringInfo(&query_create, "CREATE TEMPORARY TABLE \"%s\"(\"%s\" INT, \"%s\" BIGINT) ON COMMIT DROP DISTRIBUTED RANDOMLY;", distributed_table_name(ftoid), PARTITION_COLUMN, OFFSET_COLUMN);

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));

	PG_TRY();
	{
		PG_TRY();
		{
			execute_spi_or_error(query_create.data, false, 0);
		}
		PG_CATCH();
		{
			if (elog_geterrcode() == ERRCODE_DUPLICATE_TABLE)
			{
				FlushErrorState();
				ereport(ERROR,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("Kafka-ADB: In a single session, only one cursor per table is allowed"))
					);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();

	pfree(query_create.data);

	/* Retrieve OID */

	Relation	distributed_table = heap_openrv(makeRangeVar(temporary_namespace_name(), distributed_table_name(ftoid), -1), NoLock);

	Assert(PointerIsValid(distributed_table));
	Oid			result = RelationGetRelid(distributed_table);

	heap_close(distributed_table, NoLock);

	return result;
}

void
drop_distributed_table(Oid ftoid)
{
	ASSERT_CONTROLLER();

	StringInfoData query_drop;

	initStringInfo(&query_drop);
	appendStringInfo(&query_drop, "DROP TABLE IF EXISTS %s.\"%s\";", temporary_namespace_name(), distributed_table_name(ftoid));

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));

	PG_TRY();
	{
		execute_spi_or_error(query_drop.data, false, 0);
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();

	pfree(query_drop.data);
}

void
write_distributed_offsets(Oid dtoid, List *partition_offset_pairs)
{
	ASSERT_EXECUTOR();

	if (list_length(partition_offset_pairs) == 0)
	{
		elog(DEBUG1, "Kafka-ADB: Wrote 0 partition-offset pairs to local offsets relation (OID %u)", dtoid);
		return;
	}

	/*
	 * 'heap_' direct access is used to bypass distribution policy
	 * limitations. Normally, GPDB disallows access to non-system relations,
	 * except for REPLICATED ones.
	 */

	/* 'heap_' methods leak memory */
	MemoryContext temporary_insert_context = allocate_temporary_context_with_unique_name("offsets", gp_session_id);
	MemoryContext oldcontext = MemoryContextSwitchTo(temporary_insert_context);

	PG_TRY();
	{
		Relation	distributed_table = try_heap_open(dtoid, RowExclusiveLock, false);

		if (!PointerIsValid(distributed_table))
		{
			elog(DEBUG1, "Kafka-ADB: Failed to open the local offsets relation (OID %u); not writing offsets - must be in transaction cancellation state", dtoid);
			break;
		}
		PG_TRY();
		{
			HeapTuple  *tuples = (HeapTuple *) palloc(sizeof(HeapTuple) * list_length(partition_offset_pairs));

			ListCell   *it;
			int			i;

			foreach_with_count(it, partition_offset_pairs, i)
			{
				PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

				Datum		values[2] = {Int32GetDatum(pop->partition), Int64GetDatum(pop->offset)};
				bool		nulls[2] = {false, false};

				tuples[i] = heap_form_tuple(RelationGetDescr(distributed_table), values, nulls);
			}

			/*
			 * Insert tuples as frozen ones, effectively bypassing PostgreSQL
			 * MVCC.
			 *
			 * We drop the distributed table after transaction finish anyway,
			 * so actual XIDs used for tuples do not matter. However, when
			 * CURSORs are used with Kafka-ADB FOREIGN TABLE, master does not
			 * see tuples inserted on segment if the tuples use just
			 * 'CurrentTransactionId()'. The cause of this behaviour (which
			 * takes place only when CURSORs are used) is currently unclear.
			 */
			const int	hi_flags = HEAP_INSERT_FROZEN;

			heap_multi_insert(
							  distributed_table,
							  tuples, list_length(partition_offset_pairs),
							  GetCurrentCommandId(true),
							  hi_flags, NULL,
							  GetCurrentTransactionId()
				);

			/*
			 * This makes tuples visible to other commands (SQL queries) in
			 * the current transaction; however, since this is done on
			 * segment, this call is not propagated to master.
			 */
			CommandCounterIncrement();

			for (i = 0; i < list_length(partition_offset_pairs); i++)
				heap_freetuple(tuples[i]);
			pfree(tuples);

			elog(DEBUG1, "Kafka-ADB: Wrote %d partition-offset pairs to local offsets relation (OID %u)", list_length(partition_offset_pairs), dtoid);
		}
		PG_CATCH();
		{
			heap_close(distributed_table, RowExclusiveLock);
			PG_RE_THROW();
		}
		PG_END_TRY();
		heap_close(distributed_table, RowExclusiveLock);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(temporary_insert_context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(temporary_insert_context);
}

/**
 * Load a list of 'PartitionOffsetPair *'s from a distributed offsets table for
 * the given 'ftoid'.
 */
static List *
load_distributed_partition_offset_pairs(Oid ftoid)
{
	List	   *volatile result = NIL;

	StringInfoData query;

	initStringInfo(&query);

	/* Build query in multiple steps */
	appendStringInfo(&query, "SELECT \"%s\", \"%s\" FROM %s.\"%s\";", PARTITION_COLUMN, OFFSET_COLUMN, temporary_namespace_name(), distributed_table_name(ftoid));

	/* Between SPI_connect() and SPI_finish(), a temporary mcxt is used. */
	MemoryContext allocation_mcxt = CurrentMemoryContext;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));
	PG_TRY();
	{
		execute_spi_or_error(query.data, false, 0);
		if (SPI_tuptable == NULL)
			ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to retrieve results of '%s'", query.data)));

		for (uint64 row_i = 0; row_i < SPI_processed; row_i++)
		{
			MemoryContext oldcontext = MemoryContextSwitchTo(allocation_mcxt);
			PartitionOffsetPair *pop = (PartitionOffsetPair *) palloc(sizeof(PartitionOffsetPair));

			result = lappend(result, pop);
			MemoryContextSwitchTo(oldcontext);

			bool		is_null_partition;

			pop->partition = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row_i], SPI_tuptable->tupdesc, 1, &is_null_partition));
			Assert(!is_null_partition);

			bool		is_null_offset;

			pop->offset = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[row_i], SPI_tuptable->tupdesc, 2, &is_null_offset));
			Assert(!is_null_offset);
		}
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();

	pfree(query.data);

	return result;
}

void
update_partition_offset_pairs_with_distributed_offsets(Oid ftoid, List *partitions_absent)
{
	ASSERT_CONTROLLER();

	List *distributed_pops = load_distributed_partition_offset_pairs(ftoid);

	List *pops_to_update = NIL;
	List *pops_to_insert = NIL;

	/* Separate partitions for UPDATE and for INSERT */
	ListCell *it_pop;
	foreach(it_pop, distributed_pops)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *)lfirst(it_pop);
		ListCell *it_partition;
		bool pop_is_absent = false;

		foreach(it_partition, partitions_absent)
		{
			if (pop->partition == lfirst_int(it_partition))
			{
				pop_is_absent = true;
				break;
			}
		}

		if (pop_is_absent)
		{
			pops_to_insert = lappend(pops_to_insert, pop);
		}
		else
		{
			pops_to_update = lappend(pops_to_update, pop);
		}
	}

	update_partition_offset_pairs(ftoid, pops_to_update);
	add_partition_offset_pairs(ftoid, pops_to_insert);

	list_free_deep(distributed_pops);
}

List *
load_partition_offset_pairs(Oid ftoid, List *partitions)
{
	List	   *volatile result = NIL;

	StringInfoData query;

	initStringInfo(&query);

	/* Build query in multiple steps */
	appendStringInfo(&query, "SELECT \"%s\", \"%s\" FROM %s.\"%s\" WHERE \"%s\" = %d", PARTITION_COLUMN, OFFSET_COLUMN, EXTENSION_NAMESPACE, GLOBAL_TABLE_NAME, FTOID_COLUMN, ftoid);
	if (partitions != NIL)
	{
		appendStringInfo(&query, " AND \"%s\" IN (", PARTITION_COLUMN);
		ListCell   *it;

		foreach(it, partitions)
			appendStringInfo(&query, it != list_tail(partitions) ? "%d, " : "%d)", it->data.int_value);
	}
	appendStringInfo(&query, ";");

	/* Between SPI_connect() and SPI_finish(), a temporary mcxt is used. */
	MemoryContext allocation_mcxt = CurrentMemoryContext;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));
	PG_TRY();
	{
		execute_spi_or_error(query.data, true, 0);
		if (SPI_tuptable == NULL)
			ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to retrieve results of '%s'", query.data)));

		for (uint64 row_i = 0; row_i < SPI_processed; row_i++)
		{
			MemoryContext oldcontext = MemoryContextSwitchTo(allocation_mcxt);
			PartitionOffsetPair *pop = (PartitionOffsetPair *) palloc(sizeof(PartitionOffsetPair));

			result = lappend(result, pop);
			MemoryContextSwitchTo(oldcontext);

			bool		is_null_partition;

			pop->partition = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row_i], SPI_tuptable->tupdesc, 1, &is_null_partition));
			Assert(!is_null_partition);
			bool		is_null_offset;

			pop->offset = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[row_i], SPI_tuptable->tupdesc, 2, &is_null_offset));
			Assert(!is_null_offset);
		}
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();

	pfree(query.data);

	return result;
}

void
check_partition_offset_pairs_presence(Oid ftoid, int64_t initial_offset, List *partitions, List **partition_offset_pairs_present, List **partition_offset_pairs_absent)
{
	if (list_length(partitions) == 0)
		return;

	List	   *pops_present = load_partition_offset_pairs(ftoid, partitions);

	List	   *pops_absent = NIL;

	ListCell   *it_partition;

	foreach(it_partition, partitions)
	{
		int32_t		partition = lfirst_int(it_partition);
		ListCell   *it_pop;
		bool		partition_found = false;

		foreach(it_pop, pops_present)
		{
			int32_t		present_partition = ((PartitionOffsetPair *) lfirst(it_pop))->partition;

			if (present_partition == partition)
			{
				partition_found = true;
				break;
			}
		}

		if (!partition_found)
		{
			PartitionOffsetPair *pop_absent = (PartitionOffsetPair *) palloc(sizeof(PartitionOffsetPair));

			pops_absent = lappend(pops_absent, pop_absent);

			pop_absent->partition = partition;

			if (initial_offset >= 0)
			{
				pop_absent->offset = initial_offset;
				ereport(
						NOTICE,
						(errmsg("Kafka-ADB: Offset for partition %d is not known, and is set to default value %" PRId64, pop_absent->partition, pop_absent->offset))
					);
			}
		}
	}

	if (PointerIsValid(partition_offset_pairs_present))
		*partition_offset_pairs_present = pops_present;
	if (PointerIsValid(partition_offset_pairs_absent))
		*partition_offset_pairs_absent = pops_absent;
}

void
add_partition_offset_pairs(Oid ftoid, List *partition_offset_pairs)
{
	ASSERT_CONTROLLER();

	if (list_length(partition_offset_pairs) == 0)
		return;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));
	PG_TRY();
	{
		StringInfoData query_insert_template;

		initStringInfo(&query_insert_template);
		appendStringInfo(&query_insert_template, "INSERT INTO %s.\"%s\"(\"%s\", \"%s\", \"%s\") VALUES ", EXTENSION_NAMESPACE, GLOBAL_TABLE_NAME, FTOID_COLUMN, PARTITION_COLUMN, OFFSET_COLUMN);

		StringInfoData query_insert;

		initStringInfo(&query_insert);
		ListCell   *it;

		foreach(it, partition_offset_pairs)
		{
			PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

			resetStringInfo(&query_insert);
			appendStringInfoString(&query_insert, query_insert_template.data);
			appendStringInfo(&query_insert, "(%u, %d, %" PRId64 ");", ftoid, pop->partition, pop->offset);

			execute_spi_or_error(query_insert.data, false, 0);
		}
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();

	elog(DEBUG1, "Kafka-ADB: Added %d partition-offset pairs", list_length(partition_offset_pairs));
}

void
add_partitions(Oid ftoid, List *partitions, int64_t initial_offset)
{
	ASSERT_CONTROLLER();

	AssertArg(initial_offset >= 0);

	if (list_length(partitions) == 0)
	{
		return;
	}

	List	   *partition_offset_pairs = NIL;
	ListCell   *it;

	foreach(it, partitions)
	{
		PartitionOffsetPair *pop = palloc(sizeof(PartitionOffsetPair));

		*pop = (PartitionOffsetPair)
		{
			.partition = lfirst_int(it),
				.offset = initial_offset
		};
		partition_offset_pairs = lappend(partition_offset_pairs, pop);
	}

	add_partition_offset_pairs(ftoid, partition_offset_pairs);

	list_free_deep(partition_offset_pairs);

	elog(DEBUG1, "Kafka-ADB: Added %d new partitions", list_length(partitions));
}

void
update_partition_offset_pairs(Oid ftoid, List *partition_offset_pairs)
{
	ASSERT_CONTROLLER();

	if (partition_offset_pairs == NIL)
		return;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));
	PG_TRY();
	{
		StringInfo	query_update = makeStringInfo();

		ListCell   *it;

		foreach(it, partition_offset_pairs)
		{
			PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

			resetStringInfo(query_update);
			appendStringInfo(query_update, "UPDATE %s.\"%s\" SET \"%s\" = %" PRId64 " WHERE \"%s\" = %u AND \"%s\" = %d;", EXTENSION_NAMESPACE, GLOBAL_TABLE_NAME, OFFSET_COLUMN, pop->offset, FTOID_COLUMN, ftoid, PARTITION_COLUMN, pop->partition);

			execute_spi_or_error(query_update->data, false, 0);
		}

		pfree(query_update->data);
		pfree(query_update);
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();
}

void
delete_partition_offset_pairs(Oid ftoid, List *partitions)
{
	StringInfo	query = makeStringInfo();

	/* Build query in multiple steps */
	appendStringInfo(query, "DELETE FROM %s.\"%s\" WHERE \"%s\" = %d", EXTENSION_NAMESPACE, GLOBAL_TABLE_NAME, FTOID_COLUMN, ftoid);
	if (partitions != NIL)
	{
		appendStringInfo(query, " AND \"%s\" IN (", PARTITION_COLUMN);
		ListCell   *it;

		foreach(it, partitions)
			appendStringInfo(query, it != list_tail(partitions) ? "%d, " : "%d)", it->data.int_value);
	}
	appendStringInfo(query, ";");

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Kafka-ADB: Failed to connect to SPI")));
	PG_TRY();
	{
		execute_spi_or_error(query->data, false, 0);
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();
	SPI_finish();

	pfree(query->data);
	pfree(query);
}

List *
partition_offset_pairs_difference(List *a, List *b)
{
	ListCell   *it_a;
	ListCell   *it_b;

	List	   *result = NIL;

	foreach(it_a, a)
	{
		PartitionOffsetPair *pop_a = (PartitionOffsetPair *) lfirst(it_a);
		bool		pop_a_found_in_b = false;

		foreach(it_b, b)
		{
			PartitionOffsetPair *pop_b = (PartitionOffsetPair *) lfirst(it_b);

			if (pop_a->partition == pop_b->partition)
			{
				pop_a_found_in_b = true;
				break;
			}
		}

		if (!pop_a_found_in_b)
		{
			result = lappend(result, pop_a);
		}
	}

	return result;
}
