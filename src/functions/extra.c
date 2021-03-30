#include "extra.h"

#include <inttypes.h>

#include <access/reloptions.h>
#include <cdb/cdbvars.h>
#include <foreign/fdwapi.h>
#include <funcapi.h>
#include <nodes/nodes.h>

#include "kafka_consumer.h"
#include "kafka_functions.h"
#include "offsets.h"
#include "settings.h"
#include "utils/kadb_assert.h"
#include "utils/kadb_gp_utils.h"


Datum
kadb_partitions_obtain(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partitions = partition_list_kafka(ftoptions);

	List	   *partition_offset_pairs_absent = NIL;

	check_partition_offset_pairs_presence(
										  ftoid, defGetInt64(get_option(ftoptions, KADB_SETTING_K_INITIAL_OFFSET)), partitions,
										  NULL, &partition_offset_pairs_absent
		);

	add_partition_offset_pairs(ftoid, partition_offset_pairs_absent);

	PG_RETURN_VOID();
}

Datum
kadb_partitions_clean(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	List	   *ftoptions = get_and_validate_options(ftoid);

	/*
	 * Obtain a list of partitions that BOTH exist in Kafka and are present in
	 * the global offsets' table
	 */
	List	   *partitions_in_kafka = partition_list_kafka(ftoptions);

	List	   *partition_offset_pairs_in_kafka_present = NIL;

	check_partition_offset_pairs_presence(
										  ftoid, defGetInt64(get_option(ftoptions, KADB_SETTING_K_INITIAL_OFFSET)), partitions_in_kafka,
							   &partition_offset_pairs_in_kafka_present, NULL
		);

	list_free(partitions_in_kafka);

	/*
	 * Obtain a list of all partitions that are present in the global offsets'
	 * table
	 */
	List	   *partition_offset_pairs_present = load_partition_offset_pairs(ftoid, NIL);

	/* Find the difference */
	List	   *kafka_present_difference = partition_offset_pairs_difference(partition_offset_pairs_present, partition_offset_pairs_in_kafka_present);

	list_free_deep(partition_offset_pairs_in_kafka_present);

	if (list_length(kafka_present_difference) > 0)
	{
		List	   *kafka_present_difference_int = NIL;
		ListCell   *it;

		foreach(it, kafka_present_difference)
		{
			PartitionOffsetPair *pop = lfirst(it);

			kafka_present_difference_int = lappend_int(kafka_present_difference_int, (int) pop->partition);
		}

		delete_partition_offset_pairs(ftoid, kafka_present_difference_int);

		list_free(kafka_present_difference_int);
	}

	list_free_deep(partition_offset_pairs_present);

	PG_RETURN_VOID();
}

Datum
kadb_partitions_reset(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	delete_partition_offset_pairs(ftoid, NIL);

	List	   *partition_offset_pairs = ft_load_partitions(ftoid);

	add_partition_offset_pairs(ftoid, partition_offset_pairs);

	list_free_deep(partition_offset_pairs);

	PG_RETURN_VOID();
}

Datum
kadb_offsets_to_timestamp(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);
	int64_t		timestamp_ms = PG_GETARG_INT64(1);

	List	   *partition_offset_pairs = ft_load_offsets_at_timestamp(ftoid, timestamp_ms);

	update_partition_offset_pairs(ftoid, partition_offset_pairs);

	list_free_deep(partition_offset_pairs);

	PG_RETURN_VOID();
}

Datum
kadb_offsets_to_earliest(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	List	   *partition_offset_pairs = ft_load_offsets_earliest(ftoid);

	update_partition_offset_pairs(ftoid, partition_offset_pairs);

	list_free_deep(partition_offset_pairs);

	PG_RETURN_VOID();
}

Datum
kadb_offsets_to_latest(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	List	   *partition_offset_pairs = ft_load_offsets_latest(ftoid);

	update_partition_offset_pairs(ftoid, partition_offset_pairs);

	list_free_deep(partition_offset_pairs);

	PG_RETURN_VOID();
}

Datum
kadb_offsets_to_committed(PG_FUNCTION_ARGS)
{
	ASSERT_CONTROLLER();

	Oid			ftoid = PG_GETARG_OID(0);

	List	   *partition_offset_pairs = ft_load_offsets_committed(ftoid);

	update_partition_offset_pairs(ftoid, partition_offset_pairs);

	list_free_deep(partition_offset_pairs);

	PG_RETURN_VOID();
}
