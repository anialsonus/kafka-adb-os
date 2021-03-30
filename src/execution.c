#include "execution.h"

#include <access/xact.h>
#include <cdb/cdbvars.h>
#include <executor/executor.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <utils/faultinjector.h>
#include <utils/memutils.h>
#include <utils/rel.h>

#include "kafka_consumer.h"
#include "offsets.h"
#include "settings.h"
#include "deserialization/api.h"
#include "utils/kadb_gp_utils.h"


#define SETTINGS_FROM_NODE(node) (((ForeignScan *)node->ss.ps.plan)->fdw_private)


/**
 * A "snapshot" of settings for use during runtime. Used only by segments.
 */
typedef struct KFdwScanStateSettings
{
	int64		timeout_ms;
}	KFdwScanStateSettings;

/**
 * Kafka FDW scan state. Used only by segments.
 */
typedef struct KFdwScanState
{
	/* Partition-offset pairs */
	List	   *partition_offset_pairs_start;	/* At scan start */
	List	   *partition_offset_pairs; /* Current */

	/* librdkafka internal state */
	KafkaObjects kobj;

	/* Deserialization metadata */
	DeserializationMetadata ds_metadata;

	/* A list of tuples produced by deserializer and corresponding objects */
	List	   *prepared_tuples;
	ListCell   *prepared_tuples_it;
	MemoryContext prepared_tuples_mcxt;

	KFdwScanStateSettings settings;
}	KFdwScanState;

/**
 * Kafka FDW scan state used by master.
 */
typedef struct KFdwScanStateMaster
{
	char		_;				/* Just to make this a non-zero size structure */
}	KFdwScanStateMaster;


/**
 * Prepare the Kafka-ADB FDW scan state object for a new foreign scan.
 *
 * This method does NOT initialize librdkafka and AVRO metadata objects.
 *
 * This method EXPECTS 'kfss->partition_offset_pairs_start' to be a non-empty
 * list.
 *
 * Operations are performed in the 'CurrentMemoryContext'.
 *
 * @param ksstate 'KFdwScanState' to initialize
 * @param is_new whether 'ksstate' is a new object - and its objects must be
 * allocated by this method
 */
static void
prepare_kfdw_scanstate(KFdwScanState * ksstate, bool is_new)
{
	Assert(PointerIsValid(ksstate));

	/* Deep copy 'partition_offset_pairs_start' to 'partition_offset_pairs' */
	Assert(ksstate->partition_offset_pairs_start != NIL);
	if (is_new)
		ksstate->partition_offset_pairs = list_copy(ksstate->partition_offset_pairs_start);
	Assert(list_length(ksstate->partition_offset_pairs_start) == list_length(ksstate->partition_offset_pairs));
	ListCell   *it,
			   *it_start;

	forboth(it, ksstate->partition_offset_pairs, it_start, ksstate->partition_offset_pairs_start)
	{
		if (is_new)
			lfirst(it) = palloc(sizeof(PartitionOffsetPair));
		*(PartitionOffsetPair *) lfirst(it) = *(PartitionOffsetPair *) lfirst(it_start);
	}

	/* Prepare the storage for a batch of tuples */
	ksstate->prepared_tuples = NIL;
	ksstate->prepared_tuples_it = NULL;
	if (is_new)
		ksstate->prepared_tuples_mcxt = allocate_temporary_context_with_unique_name("prepared_tuples", (Oid) gp_session_id);
	else
		MemoryContextReset(ksstate->prepared_tuples_mcxt);
}

/**
 * 'kadbBeginForeignScan()' master instance implementation
 */
static void
kadbBeginForeignScanOnMaster(ForeignScanState *node, int eflags)
{
	/* Distinguish actual scan from pure EXPLAIN */
	node->fdw_state = palloc(sizeof(KFdwScanStateMaster));
}

/**
 * 'kadbBeginForeignScan()' segment instance implementation
 */
static void
kadbBeginForeignScanOnSegment(ForeignScanState *node, int eflags)
{
	List	   *settings;
	KFdwScanState *ksstate;

	settings = SETTINGS_FROM_NODE(node);

	/* Check if there are partitions assigned to the current segment (at all) */
	List	   *partitions_of_current_segment = (List *) list_nth((List *) (get_option(settings, KADB_SETTING__PARTITION_DISTRIBUTION)->arg), GpIdentity.segindex);

	if (partitions_of_current_segment == NIL)
	{
		node->fdw_state = NULL;
		return;
	}

	ksstate = (KFdwScanState *) palloc(sizeof(KFdwScanState));

	elog(DEBUG1, "Kafka-ADB: Loading and validating partition-offset pairs...");

	{
		List	   *pops_of_current_segment = NIL;
		List	   *pops_of_current_segment_absent = NIL;

		check_partition_offset_pairs_presence(
							   RelationGetRelid(node->ss.ss_currentRelation),
			defGetInt64(get_option(settings, KADB_SETTING_K_INITIAL_OFFSET)),
											  partitions_of_current_segment,
											  &pops_of_current_segment,
											  &pops_of_current_segment_absent
			);
		pops_of_current_segment = list_concat(pops_of_current_segment, pops_of_current_segment_absent);

		ksstate->partition_offset_pairs_start = pops_of_current_segment;
	}

	validate_partition_offset_pairs(settings, ksstate->partition_offset_pairs_start);

	prepare_kfdw_scanstate(ksstate, true);

	elog(DEBUG1, "Kafka-ADB: Initializing Kafka connection...");
	kobj_initialize_topic_connection(&ksstate->kobj, settings, ksstate->partition_offset_pairs);

	elog(DEBUG1, "Kafka-ADB: Initializing deserialization...");
	ksstate->ds_metadata = prepare_deserialization(TupleDescGetAttInMetadata(RelationGetDescr(node->ss.ss_currentRelation))->tupdesc, settings);

	ksstate->settings.timeout_ms = defGetInt64(get_option(settings, KADB_SETTING_K_TIMEOUT_MS));

	node->fdw_state = ksstate;
}

void
kadbBeginForeignScan(ForeignScanState *node, int eflags)
{
	/* In EXPLAIN, do nothing everywhere */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		elog(DEBUG1, "Kafka-ADB: FDW state is set to NULL");
		node->fdw_state = NULL;
		return;
	}

	if (Gp_role == GP_ROLE_DISPATCH)
		kadbBeginForeignScanOnMaster(node, eflags);
	else if (Gp_role == GP_ROLE_EXECUTE)
		kadbBeginForeignScanOnSegment(node, eflags);
	else
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Foreign scan for Kafka-ADB is supported only in cluster mode")));
}

/**
 * Update offset for the partition in 'partition_offset_pairs', taking into
 * account the given 'message'.
 *
 * Currently, this method simply iterates through all 'partition_offset_pairs'
 * and compares the offsets and partitions found with the ones in 'message'.
 */
static void
update_offset(List *partition_offset_pairs, rd_kafka_message_t * message)
{
	Assert(PointerIsValid(message));

	if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR)
		return;

	PartitionOffsetPair updated = {
		.partition = message->partition,

		/*
		 * The offsets stored in offsets' table refer to offsets of *next*
		 * messages to read, not the already read ones.
		 */
		.offset = message->offset + 1
	};

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
	{
		/*
		 * For testing, offset of the first partition only is increased.
		 */
		if (list_length(partition_offset_pairs) > 0)
		{
			PartitionOffsetPair *to_update = (PartitionOffsetPair *) lfirst(list_head(partition_offset_pairs));

			to_update->offset += 1;
		}
		return;
	}
#endif

	ListCell   *it;

	foreach(it, partition_offset_pairs)
	{
		PartitionOffsetPair *current = (PartitionOffsetPair *) lfirst(it);

		if ((current->partition == updated.partition) && (current->offset < updated.offset))
		{
			current->offset = updated.offset;
			break;
		}
	}
}

TupleTableSlot *
kadbIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *volatile slot = node->ss.ss_ScanTupleSlot;

	/* Master does not perform actual scan */
	if (Gp_role == GP_ROLE_DISPATCH)
		return ExecClearTuple(slot);
	/* In EXPLAIN or with no partition-offset pairs assigned */
	if (!PointerIsValid(node->fdw_state))
		return ExecClearTuple(slot);

	KFdwScanState *ksstate = node->fdw_state;

	while (!PointerIsValid(ksstate->prepared_tuples_it))
	{
		rd_kafka_message_t *message = fetch_message(ksstate->kobj);

		/* Check if the loop must be finished */
		if (!PointerIsValid(message))
			return ExecClearTuple(slot);

		PG_TRY();
		{
			{
				MemoryContextReset(ksstate->prepared_tuples_mcxt);
				MemoryContext oldcontext = MemoryContextSwitchTo(ksstate->prepared_tuples_mcxt);

				ksstate->prepared_tuples = deserialize(ksstate->ds_metadata, message->payload, message->len);
				MemoryContextSwitchTo(oldcontext);
			}
			if (list_length(ksstate->prepared_tuples) > 0)
				ksstate->prepared_tuples_it = list_head(ksstate->prepared_tuples);

			/* Update offset by the processed message */
			update_offset(ksstate->partition_offset_pairs, message);
		}
		PG_CATCH();
		{
#ifdef FAULT_INJECTOR
			if (!(SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip))
#endif
				rd_kafka_message_destroy(message);

			PG_RE_THROW();
		}
		PG_END_TRY();

#ifdef FAULT_INJECTOR
		if (!(SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip))
#endif
			rd_kafka_message_destroy(message);
	}

	MemoryContext oldcontext = MemoryContextSwitchTo(PortalContext);

	/*
	 * We use 'heap_copytuple()' in order to safely reset context which
	 * contains "original" parsed values. 'ExecStoreHeapTuple()' is called
	 * with last parameter set, thus GPDB is taking ownership of the returned
	 * tuple.
	 */
	ExecStoreHeapTuple(heap_copytuple(lfirst(ksstate->prepared_tuples_it)), slot, InvalidBuffer, true);
	MemoryContextSwitchTo(oldcontext);

	ksstate->prepared_tuples_it = lnext(ksstate->prepared_tuples_it);

	return slot;
}

void
kadbReScanForeignScan(ForeignScanState *node)
{
	/* Master does not perform any "temporary" actions */
	if (Gp_role == GP_ROLE_DISPATCH)
		return;
	/* In EXPLAIN or with no partition-offset pairs assigned */
	if (!PointerIsValid(node->fdw_state))
		return;

	KFdwScanState *ksstate = node->fdw_state;

	prepare_kfdw_scanstate(ksstate, false);

	/*
	 * Resetting topic partitions is a complicated task.
	 *
	 * It seems more reliable to recreate a queue and resubscribe it to target
	 * partitions rather than flush offsets and free queue manually.
	 */
	Assert(PointerIsValid(ksstate->kobj));
	kobj_restart(ksstate->kobj, ksstate->partition_offset_pairs);
}

/**
 * 'kadbEndForeignScan()' master instance implementation
 */
static void
kadbEndForeignScanOnMaster(ForeignScanState *node)
{
	if (!IsTransactionState())
		return;

	List	   *settings = SETTINGS_FROM_NODE(node);

	elog(DEBUG1, "Kafka-ADB: Updating offsets...");
	update_partition_offset_pairs_with_distributed_offsets(
		RelationGetRelid(node->ss.ss_currentRelation),
		((List *)get_option(settings, KADB_SETTING__PARTITIONS_ABSENT)->arg)
	);

	elog(DEBUG1, "Kafka-ADB: Dropping temporary distributed offsets relation...");
	drop_distributed_table(RelationGetRelid(node->ss.ss_currentRelation));
}

/**
 * 'kadbEndForeignScan()' segment instance implementation
 */
static void
kadbEndForeignScanOnSegment(ForeignScanState *node)
{
	KFdwScanState *ksstate = node->fdw_state;

	if (PointerIsValid(ksstate->kobj))
	{
		elog(DEBUG1, "Kafka-ADB: Destroying Kafka connection...");
		kobj_finish_and_destroy(ksstate->kobj, ksstate->partition_offset_pairs);
		pfree(ksstate->kobj);
	}
	if (PointerIsValid(ksstate->ds_metadata))
	{
		elog(DEBUG1, "Kafka-ADB: Finishing deserialization...");
		finish_deserialization(ksstate->ds_metadata);
	}

	if (!IsTransactionState())
		return;

	List	   *settings = SETTINGS_FROM_NODE(node);

	elog(DEBUG1, "Kafka-ADB: Writing updated partition-offset pairs to local offsets relation...");
	write_distributed_offsets(defGetInt64(get_option(settings, KADB_SETTING__DISTRIBUTED_TABLE)), ksstate->partition_offset_pairs);
}

void
kadbEndForeignScan(ForeignScanState *node)
{
	/* In EXPLAIN or on segment with no partition-offset pairs assigned */
	if (!PointerIsValid(node->fdw_state))
		return;

	if (Gp_role == GP_ROLE_DISPATCH)
		kadbEndForeignScanOnMaster(node);
	else if (Gp_role == GP_ROLE_EXECUTE)
		kadbEndForeignScanOnSegment(node);
	else
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Foreign scan for Kafka-ADB is supported only in cluster mode")));
}
