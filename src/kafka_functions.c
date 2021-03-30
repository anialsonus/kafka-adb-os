#include "kafka_functions.h"

#include <nodes/pg_list.h>

#include "settings.h"
#include "offsets.h"
#include "kafka_consumer.h"
#include "utils/kadb_assert.h"


void
ft_commit_offsets(Oid ftoid)
{
	ASSERT_CONTROLLER();

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partition_offset_pairs = load_partition_offset_pairs(ftoid, NIL);

	commit_offsets(ftoptions, partition_offset_pairs);
}

List *
ft_load_offsets_at_timestamp(Oid ftoid, int64_t timestamp_ms)
{
	ASSERT_CONTROLLER();

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partition_offset_pairs = load_partition_offset_pairs(ftoid, NIL);

	offsets_to_timestamp(ftoptions, partition_offset_pairs, timestamp_ms);

	return partition_offset_pairs;
}

List *
ft_load_offsets_earliest(Oid ftoid)
{
	ASSERT_CONTROLLER();

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partition_offset_pairs = load_partition_offset_pairs(ftoid, NIL);

	offsets_to_earliest(ftoptions, partition_offset_pairs);

	return partition_offset_pairs;
}

List *
ft_load_offsets_latest(Oid ftoid)
{
	ASSERT_CONTROLLER();

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partition_offset_pairs = load_partition_offset_pairs(ftoid, NIL);

	offsets_to_latest(ftoptions, partition_offset_pairs);

	return partition_offset_pairs;
}

List *
ft_load_offsets_committed(Oid ftoid)
{
	ASSERT_CONTROLLER();

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partition_offset_pairs = load_partition_offset_pairs(ftoid, NIL);

	offsets_to_committed(ftoptions, partition_offset_pairs);

	return partition_offset_pairs;
}

List *
ft_load_partitions(Oid ftoid)
{
	ASSERT_CONTROLLER();

	List	   *ftoptions = get_and_validate_options(ftoid);

	List	   *partitions = partition_list_kafka(ftoptions);

	List	   *partition_offset_pairs_present = NIL;
	List	   *partition_offset_pairs_absent = NIL;

	check_partition_offset_pairs_presence(
										  ftoid, defGetInt64(get_option(ftoptions, KADB_SETTING_K_INITIAL_OFFSET)), partitions,
			  &partition_offset_pairs_present, &partition_offset_pairs_absent
		);

	return list_concat(partition_offset_pairs_present, partition_offset_pairs_absent);
}
