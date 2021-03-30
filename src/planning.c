#include "planning.h"

#include <cdb/cdbutil.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <optimizer/cost.h>
#include <optimizer/pathnode.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include <utils/faultinjector.h>

#include "kafka_consumer.h"
#include "offsets.h"
#include "settings.h"


/**
 * A state used during query planning.
 */
typedef struct KAdbFdwPlanState
{
	/* Data that must be passed to executors */
	List	   *execution_data;
	/* Number of partitions to select data from */
	int			partitions;
}	KAdbFdwPlanState;


/**
 * Get all partitions already present in the offsets' table.
 *
 * @return a list of Int
 */
static List *
get_existing_partition_offset_pairs(Oid ftoid, List *options)
{
	List	   *partition_offset_pairs = load_partition_offset_pairs(ftoid, NIL);

	List	   *result = NIL;

	ListCell   *it;

	foreach(it, partition_offset_pairs)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		result = lappend_int(result, pop->partition);
	}

	list_free_deep(partition_offset_pairs);
	return result;
}


#ifdef FAULT_INJECTOR
/**
 * Directive for 'partition_list_dummy()': how many partitions to return per
 * segment.
 */
enum DummyPartitionListDirective
{
	DUMMY_PARTITION_LIST_DIRECTIVE_NONE,
	DUMMY_PARTITION_LIST_DIRECTIVE_05_PER_SEGMENT,
	DUMMY_PARTITION_LIST_DIRECTIVE_1_PER_SEGMENT,
	DUMMY_PARTITION_LIST_DIRECTIVE_2_PER_SEGMENT,
};

/**
 * Generate a list of partitions using 'directive' and taking into account the
 * number of segments in cluster.
 *
 * @return a list of Int
 */
static List *
partition_list_dummy(enum DummyPartitionListDirective directive)
{
	List	   *result = NIL;

	int			segment_count = getgpsegmentCount();
	int			part_i = 0;
	bool		do_break = false;

	while (true)
	{
		switch (directive)
		{
			case DUMMY_PARTITION_LIST_DIRECTIVE_NONE:
				do_break = true;
				break;
			case DUMMY_PARTITION_LIST_DIRECTIVE_05_PER_SEGMENT:
				if (part_i >= segment_count / 2)
					do_break = true;
				break;
			case DUMMY_PARTITION_LIST_DIRECTIVE_1_PER_SEGMENT:
				if (part_i >= segment_count)
					do_break = true;
				break;
			case DUMMY_PARTITION_LIST_DIRECTIVE_2_PER_SEGMENT:
				if (part_i >= segment_count * 2)
					do_break = true;
				break;
			default:
				elog(ERROR, "Invalid DummyPartitionListDirective %d", (int) directive);
		}

		if (do_break)
			break;

		result = lappend_int(result, part_i++);
	}

	return result;
}
#endif


/**
 * Get a list of partitions to SELECT data from, for the given 'ftoid'.
 *
 * @return a list of Int
 */
static List *
get_partition_list(Oid ftoid, List *options)
{
	if (!defGetBoolean(get_option(options, KADB_SETTING_K_AUTOMATIC_OFFSETS)))
	{
		elog(DEBUG1, "Kafka-ADB: Existing partition-offset pairs are used");
		return get_existing_partition_offset_pairs(ftoid, options);
	}
	elog(DEBUG1, "Kafka-ADB: Partition-offset pairs are loaded from Kafka");

	List	   *result = NIL;

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_partition_none") == FaultInjectorTypeSkip)
		result = partition_list_dummy(DUMMY_PARTITION_LIST_DIRECTIVE_NONE);
	else if (SIMPLE_FAULT_INJECTOR("kadb_fdw_partition_05_per_segment") == FaultInjectorTypeSkip)
		result = partition_list_dummy(DUMMY_PARTITION_LIST_DIRECTIVE_05_PER_SEGMENT);
	else if (SIMPLE_FAULT_INJECTOR("kadb_fdw_partition_1_per_segment") == FaultInjectorTypeSkip)
		result = partition_list_dummy(DUMMY_PARTITION_LIST_DIRECTIVE_1_PER_SEGMENT);
	else if (SIMPLE_FAULT_INJECTOR("kadb_fdw_partition_2_per_segment") == FaultInjectorTypeSkip)
		result = partition_list_dummy(DUMMY_PARTITION_LIST_DIRECTIVE_2_PER_SEGMENT);
	else
#endif
		result = partition_list_kafka(options);

	return result;
}

/**
 * Form a partition distribution list.
 *
 * @param ftoid FOREIGN TABLE OID
 * @param options FOREIGN TABLE options
 * @param partition_count set to total number of partitions, if not NULL
 *
 * Partition distribution rules are as follows:
 *
 * 1. Partitions are distributed in equal proportions among all segments. The
 * actual number of partitions of a single segment varies by 1 among all
 * segments.
 *
 * 2. Partitions are assigned to segments in the same order in which they are
 * returned.
 *
 * 3. If some segment is assigned zero partitions, the resulting list contains
 * an entry list for this segment which is empty (is NIL)
 *
 * Examples:
 *
 * Suppose there is a GPDB cluster with 3 segments.
 *
 * Kafka partition listing returns a list [1, 2, 3, 4, 5]. Then, the resulting
 * distribution list is [[1, 2], [3, 4], [5]].
 *
 * Kafka partition listing returns a list [1, 2, 3, 4]. Then, the resulting
 * distribution list is [[1, 2], [3], [4]].
 *
 * Kafka partition listing returns a list [1, 2]. Then, the resulting
 * distribution list is [[1], [2], NIL].
 */
static List *
get_partition_distribution(List *partitions)
{
	List	   *result = NIL;

	int			total_segments = getgpsegmentCount();
	int			partitions_per_segment = list_length(partitions) / total_segments;
	int			segments_with_extra_partition = list_length(partitions) % total_segments;

	/* Each segment gets its own list of partitions */
	for (int i = 0; i < total_segments; i++)
	{
		result = lappend(result, NIL);
	}

	int			seg_i = 0;
	ListCell   *seg_it = list_head(result);
	int			last_segment_partitions_count = 0;
	ListCell   *part_it;

	foreach(part_it, partitions)
	{
		int			part = lfirst_int(part_it);

		lfirst(seg_it) = lappend_int((List *) lfirst(seg_it), part);
		last_segment_partitions_count += 1;

		if (
			(last_segment_partitions_count >= partitions_per_segment)
			&& (
				seg_i >= segments_with_extra_partition ||
				last_segment_partitions_count > partitions_per_segment
				)
			)
		{
			seg_i += 1;
			seg_it = lnext(seg_it);
			last_segment_partitions_count = 0;
		}
	}

	return result;
}

/**
 * Return a list of partitions from 'partitions' absent in the global offsets'
 * table.
 */
static List *
get_absent_partitions(Oid ftoid, List *partitions)
{
	List	   *partition_offset_pairs_absent = NIL;

	check_partition_offset_pairs_presence(ftoid, -1, partitions, NULL, &partition_offset_pairs_absent);

	List	   *result = NIL;
	ListCell   *it;

	foreach(it, partition_offset_pairs_absent)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		result = lappend_int(result, pop->partition);
	}

	list_free_deep(partition_offset_pairs_absent);

	return result;
}

void
kadbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	KAdbFdwPlanState *pstate;
	List	   *options;

	pstate = (KAdbFdwPlanState *) palloc(sizeof(KAdbFdwPlanState));

	/* Options are used in the following calls */
	options = get_and_validate_options(foreigntableid);

	/* Form partitions */
	List	   *partitions = get_partition_list(foreigntableid, options);

	if (list_length(partitions) == 0)
	{
		ereport(
				WARNING,
				(errmsg("Kafka-ADB: Found no partitions in topic '%s'", defGetString(get_option(options, KADB_SETTING_K_TOPIC))))
			);
	}

	options = lappend(options, makeDefElem(KADB_SETTING__PARTITION_DISTRIBUTION, (Node *) get_partition_distribution(partitions)));
	options = lappend(options, makeDefElem(KADB_SETTING__PARTITIONS_ABSENT, (Node *) get_absent_partitions(foreigntableid, partitions)));

	/* CREATE a distributed offsets' table */
	options = lappend(options, makeDefElem(KADB_SETTING__DISTRIBUTED_TABLE, (Node *) makeInteger(create_distributed_table(foreigntableid))));

	/* Set FDW objects */
	pstate->execution_data = options;
	pstate->partitions = list_length(partitions);
	baserel->rows = pstate->partitions; /* TODO: Improve row count calculation */
	baserel->fdw_private = pstate;

	list_free(partitions);
}

void
kadbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	KAdbFdwPlanState *pstate = (KAdbFdwPlanState *) baserel->fdw_private;
	Cost		startup_cost;
	Cost		run_cost;
	Cost		total_cost;

	/* TODO: Improve cost calculation */
	startup_cost = baserel->baserestrictcost.startup;
	run_cost = random_page_cost * pstate->partitions * 10;
	total_cost = startup_cost + run_cost;

	add_path(baserel, (Path *) create_foreignscan_path(
													   root,
													   baserel,
													   baserel->rows,
													   startup_cost,
													   total_cost,
													   NIL,
													 baserel->lateral_relids,
													   pstate->execution_data
													   ));
}

ForeignScan *
kadbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses)
{
	return make_foreignscan(
							tlist,
							extract_actual_clauses(scan_clauses, false),
							baserel->relid,
							NIL,
							best_path->fdw_private
		);
}
