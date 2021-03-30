#include "auxiliary.h"

#include <inttypes.h>

#include <access/reloptions.h>
#include <cdb/cdbvars.h>
#include <foreign/fdwapi.h>
#include <funcapi.h>
#include <nodes/nodes.h>

#include "kafka_functions.h"
#include "offsets.h"
#include "settings.h"
#include "utils/kadb_assert.h"


/* Maximum length of a string representation of int64_t */
#define NUMBER_PRINT_SIZE 21


Datum
kadb_commit_offsets(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	ft_commit_offsets(ftoid);

	PG_RETURN_VOID();
}

/**
 * Convert a list of 'partition_offset_pairs' into a list of 'Datum *'. Each
 * Datum in the resulting list is allocated in 'CurrentMemoryContext'.
 *
 * 'fcc->tuple_desc', 'fcc->attinmeta', and 'fcc->maxcalls' are set to proper
 * values by this function.
 */
static List *
partition_offset_pairs_to_datums(Oid ftoid, List *partition_offset_pairs, FunctionCallInfo fcinfo, FuncCallContext *fcc)
{
	get_call_result_type(fcinfo, NULL, &fcc->tuple_desc);
	fcc->tuple_desc = BlessTupleDesc(fcc->tuple_desc);
	fcc->attinmeta = TupleDescGetAttInMetadata(fcc->tuple_desc);

	/* Prepare a container for the result */
	char	   *values[3];
	int			i;

	for (i = 0; i < 3; i++)
	{
		values[i] = palloc(NUMBER_PRINT_SIZE);
	}

	List	   *result = NIL;
	ListCell   *it;

	foreach(it, partition_offset_pairs)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);
		int			err;

		err = snprintf(values[0], NUMBER_PRINT_SIZE, "%u", ftoid);
		Assert(err > 0 && err < NUMBER_PRINT_SIZE);
		err = snprintf(values[1], NUMBER_PRINT_SIZE, "%d", pop->partition);
		Assert(err > 0 && err < NUMBER_PRINT_SIZE);
		err = snprintf(values[2], NUMBER_PRINT_SIZE, "%" PRId64, pop->offset);
		Assert(err > 0 && err < NUMBER_PRINT_SIZE);

		Datum	   *result_element = palloc(sizeof(Datum));

		*result_element = HeapTupleGetDatum(BuildTupleFromCStrings(fcc->attinmeta, (char **) values));

		result = lappend(result, result_element);
	}

	for (i = 0; i < 3; i++)
	{
		pfree(values[i]);
	}

	fcc->max_calls = list_length(result);

	return result;
}

Datum
kadb_load_offsets_at_timestamp(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	FuncCallContext *fcc;

	if (SRF_IS_FIRSTCALL())
	{
		fcc = SRF_FIRSTCALL_INIT();

		MemoryContext oldcontext = MemoryContextSwitchTo(fcc->multi_call_memory_ctx);

		Oid			ftoid = PG_GETARG_OID(0);
		int64_t		timestamp_ms = PG_GETARG_INT64(1);

		fcc->user_fctx = (void *) partition_offset_pairs_to_datums(ftoid, ft_load_offsets_at_timestamp(ftoid, timestamp_ms), fcinfo, fcc);

		MemoryContextSwitchTo(oldcontext);
	}

	fcc = SRF_PERCALL_SETUP();

	List	   *resultset = (List *) fcc->user_fctx;

	if (fcc->call_cntr >= fcc->max_calls)
	{
		list_free_deep(resultset);
		SRF_RETURN_DONE(fcc);
	}

	Datum		result = *(Datum *) lfirst(list_nth_cell(resultset, fcc->call_cntr));

	SRF_RETURN_NEXT(fcc, result);
}

/**
 * A "template" for several set-returning functions.
 *
 * @param fn a function returning a list of 'PartitionOffsetPair *'. It is
 * called exactly once, on the first call.
 */
static Datum
single_argument_set_returning_function(PG_FUNCTION_ARGS, List *(*fn) (Oid))
{
	FuncCallContext *fcc;

	if (SRF_IS_FIRSTCALL())
	{
		fcc = SRF_FIRSTCALL_INIT();

		MemoryContext oldcontext = MemoryContextSwitchTo(fcc->multi_call_memory_ctx);

		Oid			ftoid = PG_GETARG_OID(0);

		fcc->user_fctx = (void *) partition_offset_pairs_to_datums(ftoid, fn(ftoid), fcinfo, fcc);

		MemoryContextSwitchTo(oldcontext);
	}

	fcc = SRF_PERCALL_SETUP();

	List	   *resultset = (List *) fcc->user_fctx;

	if (fcc->call_cntr >= fcc->max_calls)
	{
		list_free_deep(resultset);
		SRF_RETURN_DONE(fcc);
	}

	Datum		result = *(Datum *) lfirst(list_nth_cell(resultset, fcc->call_cntr));

	SRF_RETURN_NEXT(fcc, result);
}

Datum
kadb_load_offsets_earliest(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	return single_argument_set_returning_function(fcinfo, ft_load_offsets_earliest);
}

Datum
kadb_load_offsets_latest(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	return single_argument_set_returning_function(fcinfo, ft_load_offsets_latest);
}

Datum
kadb_load_offsets_committed(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	return single_argument_set_returning_function(fcinfo, ft_load_offsets_committed);
}

Datum
kadb_load_partitions(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	return single_argument_set_returning_function(fcinfo, ft_load_partitions);
}
