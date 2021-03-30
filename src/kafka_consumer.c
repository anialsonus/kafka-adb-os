#include "kafka_consumer.h"

#include <stdint.h>
#include <inttypes.h>

#include <miscadmin.h>
#include <nodes/pg_list.h>
#include <utils/faultinjector.h>

#include "settings.h"


/* Maximum number of metadata retrieval attempts */
#define METADATA_RETRIEVAL_ATTEMPTS_MAX 2


#define ERROR_KAFKA_CONF_SETUP_FAILED(kafka_setting, adb_setting, errstr) ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to set '%s' Kafka configuration parameter (taken from option '%s'): %s", kafka_setting, adb_setting, errstr)))

#define RD_KAFKA_CONF_SET_AND_CHECK(kafka_conf, kafka_setting, options, option_name, errstr) \
if (rd_kafka_conf_set(kafka_conf, kafka_setting, defGetString(get_option(options, option_name)), errstr, sizeof(errstr))) \
	ERROR_KAFKA_CONF_SETUP_FAILED(kafka_setting, option_name, errstr)

#define RD_KAFKA_CONF_SET_AND_CHECK_OPTIONAL(kafka_conf, kafka_setting, options, option_name, errstr) \
if (get_option(options, option_name)) \
	RD_KAFKA_CONF_SET_AND_CHECK(kafka_conf, kafka_setting, options, option_name, errstr)

#define RD_KAFKA_CONF_SET_CONSTANT(kafka_conf, kafka_setting, kafka_setting_value, errstr) \
if (rd_kafka_conf_set(kafka_conf, kafka_setting, kafka_setting_value, errstr, sizeof(errstr))) \
	ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to set '%s' Kafka configuration parameter: %s", kafka_setting, errstr)))

/**
 * Form a message in StringInfo, perform 'action', then 'ereport(ERROR, ...)'
 */
#define DO_THEN_ERROR(action, message...) \
do { \
	StringInfo do_then_error_temporary_string_info = makeStringInfo(); \
	appendStringInfo(do_then_error_temporary_string_info, message); \
	action; \
	ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("%s", do_then_error_temporary_string_info->data))); \
} while (false)


/* Definition is in the header */
struct KafkaObjects
{
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_queue_t *rkqu;

	KafkaRequestContext context;
};

/**
 * An "empty" struct KafkaObjects
 */
static const struct KafkaObjects KafkaObjectsEmptyStruct = {
	NULL, NULL, NULL,
	(KafkaRequestContext) {
		false, 0,
		NULL, 0, 0, 0
#ifdef FAULT_INJECTOR
		,0
#endif
	}
};


/**
 * Create a librdkafka consumer object.
 *
 * If an error happens, it is logged. No objects need to be destroyed in this
 * case.
 */
static rd_kafka_t *
kafka_create_consumer(List *options)
{
	char		errstr[512];

	rd_kafka_conf_t *conf = rd_kafka_conf_new();

	RD_KAFKA_CONF_SET_AND_CHECK(conf, "bootstrap.servers", options, KADB_SETTING_K_BROKERS, errstr);
	RD_KAFKA_CONF_SET_AND_CHECK(conf, "group.id", options, KADB_SETTING_K_CONSUMER_GROUP, errstr);
	RD_KAFKA_CONF_SET_AND_CHECK(conf, "socket.timeout.ms", options, KADB_SETTING_K_TIMEOUT_MS, errstr);

	RD_KAFKA_CONF_SET_CONSTANT(conf, "enable.auto.commit", "false", errstr);

	/*
	 * An explicit notification of partition EOF is better than its absence.
	 * Although this may cause a situation described in README, when
	 * 'k_seg_batch' is less than the # of partitions on some GPDB segment,
	 * that case is quite extreme. Knowledge whether the partition has been
	 * read completely or not seems to be more valuable currently.
	 */
	RD_KAFKA_CONF_SET_CONSTANT(conf, "enable.partition.eof", "true", errstr);

	RD_KAFKA_CONF_SET_AND_CHECK_OPTIONAL(conf, "security.protocol", options, KADB_SETTING_K_SECURITY_PROTOCOL, errstr);
	RD_KAFKA_CONF_SET_AND_CHECK_OPTIONAL(conf, "sasl.kerberos.keytab", options, KADB_SETTING_KERBEROS_KEYTAB, errstr);
	RD_KAFKA_CONF_SET_AND_CHECK_OPTIONAL(conf, "sasl.kerberos.principal", options, KADB_SETTING_KERBEROS_PRINCIPAL, errstr);
	RD_KAFKA_CONF_SET_AND_CHECK_OPTIONAL(conf, "sasl.kerberos.service.name", options, KADB_SETTING_KERBEROS_SERVICE_NAME, errstr);
	RD_KAFKA_CONF_SET_AND_CHECK_OPTIONAL(conf, "sasl.kerberos.min.time.before.relogin", options, KADB_SETTING_KERBEROS_MIN_TIME_BEFORE_RELOGIN, errstr);

	/* From this point, 'conf' is owned by consumer */
	rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

	if (!PointerIsValid(rk))
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to create a Kafka consumer: %s", errstr)));

	return rk;
}

/**
 * Create a librdkafka topic object by subscribing to a topic defined in
 * 'options'.
 *
 * If an error happens, it is logged. 'rk' may be considered unchanged in this
 * case.
 */
static rd_kafka_topic_t *
kafka_create_topic(rd_kafka_t * rk, List *options)
{
	rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, defGetString(get_option(options, KADB_SETTING_K_TOPIC)), NULL);

	if (!PointerIsValid(rkt))
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Failed to create a Kafka topic: %s [%d]", rd_kafka_err2str(rd_kafka_last_error()), rd_kafka_last_error())));

	return rkt;
}

/**
 * Create a librdkafka queue object by subscribing to 'partition_offset_pairs'
 * of topic 'rkt'.
 *
 * If an error happens, it is logged; temporary objects created by this function
 * are destroyed, thus 'rk' and 'rkt' may be considered unchanged in this case.
 */
static rd_kafka_queue_t *
kafka_create_queue(rd_kafka_t * rk, rd_kafka_topic_t * rkt, List *partition_offset_pairs)
{
	if (list_length(partition_offset_pairs) == 0)
		return NULL;

	rd_kafka_queue_t *rkqu;
	ListCell   *it;

	rkqu = rd_kafka_queue_new(rk);

	size_t		partitions_added_to_queue = 0;
	StringInfo	error = NULL;

	foreach(it, partition_offset_pairs)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		if (rd_kafka_consume_start_queue(rkt, pop->partition, pop->offset, rkqu))
		{
			/* An error happened */
			error = makeStringInfo();
			appendStringInfo(error, "Kafka-ADB: Failed to start a queue for partition %d, offset %" PRId64 ": %s [%d]", pop->partition, pop->offset, rd_kafka_err2str(rd_kafka_last_error()), rd_kafka_last_error());
			break;
		}
		partitions_added_to_queue += 1;
	}

	if (PointerIsValid(error))
	{
		/* Stop consumption of all partitions for which it has started */
		size_t		partitions_removed_from_queue = 0;

		foreach(it, partition_offset_pairs)
		{
			if (partitions_removed_from_queue >= partitions_added_to_queue)
				break;

			PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

			rd_kafka_consume_stop(rkt, pop->partition);
			partitions_removed_from_queue += 1;
		}

		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("%s", error->data)));
	}

	return rkqu;
}

/**
 * Finish data consumption for all partitions in 'partition_offset_pairs'.
 */
static void
finish_consumption(rd_kafka_topic_t * rkt, List *partition_offset_pairs)
{
	ListCell   *it;

	foreach(it, partition_offset_pairs)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		if (rd_kafka_consume_stop(rkt, pop->partition))
		{
			rd_kafka_resp_err_t err = rd_kafka_last_error();

			ereport(WARNING, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to stop consumption for partition %d: %s [%d]", pop->partition, rd_kafka_err2str(err), err)));
		}
	}
}

/**
 * The implementation of 'kobj_finish_and_destroy'. When 'do_finish' is set,
 * 'finish_consumption()' is called before 'rd_kafka_..._destroy()' calls.
 *
 * In case 'do_finish' is false, 'partition_offset_pairs' may be NULL.
 */
static void
kobj_destroy(KafkaObjects kobj, List *partition_offset_pairs, bool do_finish)
{
	if (!PointerIsValid(kobj))
		return;

	if (do_finish && PointerIsValid(kobj->rk) && PointerIsValid(kobj->rkt))
		finish_consumption(kobj->rkt, partition_offset_pairs);

	if (PointerIsValid(kobj->context.batch))
		pfree(kobj->context.batch);

	if (PointerIsValid(kobj->rkqu))
	{
		rd_kafka_queue_destroy(kobj->rkqu);
		kobj->rkqu = NULL;
	}
	if (PointerIsValid(kobj->rkt))
	{
		rd_kafka_topic_destroy(kobj->rkt);
		kobj->rkt = NULL;
	}
	if (PointerIsValid(kobj->rk))
	{
		rd_kafka_destroy(kobj->rk);
		kobj->rk = NULL;
	}
}

/**
 * Initialize 'context'.
 *
 * An array of pointers of size 'batch_size' will be allocated in
 * 'CurrentMemoryContext'!
 */
static void
initialize_request_context(KafkaRequestContext * context, ssize_t batch_size, int32_t timeout)
{
	Assert(PointerIsValid(context));
	Assert(batch_size > 0);
	Assert(timeout >= 0);

	context->request_made = false;
	context->request_timeout = timeout;

	context->batch = (rd_kafka_message_t * *) palloc(sizeof(*context->batch) * batch_size);
	context->batch_size = batch_size;
	context->batch_size_consumed = 0;
	context->batch_i = 0;
}

/**
 * Initialize 'kobj' and create a topic object according to 'options'.
 * If an error happens, it is logged, and 'kobj' is de-initialized.
 */
static void
kobj_initialize(KafkaObjects kobj, List *options)
{
	PG_TRY();
	{
		kobj->rk = kafka_create_consumer(options);
		kobj->rkt = kafka_create_topic(kobj->rk, options);
	}
	PG_CATCH();
	{
		kobj_destroy(kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
kobj_initialize_topic_connection(KafkaObjects * kobj_ptr, List *options, List *partition_offset_pairs)
{
	Assert(PointerIsValid(kobj_ptr));
	Assert(PointerIsValid(options));
	Assert(PointerIsValid(partition_offset_pairs));

	*kobj_ptr = (KafkaObjects) palloc(sizeof(struct KafkaObjects));

	KafkaObjects kobj = *kobj_ptr;

	*kobj = KafkaObjectsEmptyStruct;

	initialize_request_context(
							   &kobj->context,
				  defGetInt64(get_option(options, KADB_SETTING_K_SEG_BATCH)),
				  defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS))
		);
#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
	{
		kobj->context.inject_tuples_per_batch = defGetInt64(get_option(options, KADB_SETTING_K_TUPLES_PER_PARTITION_ON_INJECT));
		return;
	}
#endif
	kobj_initialize(kobj, options);
	PG_TRY();
	{
		kobj->rkqu = kafka_create_queue(kobj->rk, kobj->rkt, partition_offset_pairs);
	}
	PG_CATCH();
	{
		kobj_destroy(kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
kobj_finish_and_destroy(KafkaObjects kobj, List *partition_offset_pairs)
{
#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
		return;
#endif

	if (PointerIsValid(kobj->context.batch))
	{
		while (kobj->context.batch_i < kobj->context.batch_size_consumed)
		{
			rd_kafka_message_destroy(kobj->context.batch[kobj->context.batch_i++]);
		}
	}

	kobj_destroy(kobj, partition_offset_pairs, true);
}

void
kobj_restart(KafkaObjects kobj, List *partition_offset_pairs)
{
	Assert(PointerIsValid(kobj));
	Assert(PointerIsValid(kobj->rk) && PointerIsValid(kobj->rkt));

	kobj->context.request_made = false;
	if (PointerIsValid(kobj->context.batch))
	{
		while (kobj->context.batch_i < kobj->context.batch_size_consumed)
		{
			rd_kafka_message_destroy(kobj->context.batch[kobj->context.batch_i++]);
		}
	}
	kobj->context.batch_size_consumed = 0;
	kobj->context.batch_i = 0;

	finish_consumption(kobj->rkt, partition_offset_pairs);
	if (PointerIsValid(kobj->rkqu))
	{
		rd_kafka_queue_destroy(kobj->rkqu);
		kobj->rkqu = NULL;
	}

	PG_TRY();
	{
		kobj->rkqu = kafka_create_queue(kobj->rk, kobj->rkt, partition_offset_pairs);
	}
	PG_CATCH();
	{
		kobj_destroy(kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

#ifdef FAULT_INJECTOR
/**
 * A method to replace 'kafka_consume' for testing (eliminating the need to have
 * an actual Kafka cluster running).
 */
static void
kafka_consume_dummy(KafkaObjects kobj)
{
	for (ssize_t i = 0; i < kobj->context.inject_tuples_per_batch; i++)
	{
		kobj->context.batch[i] = (rd_kafka_message_t *) palloc(sizeof(rd_kafka_message_t));
		kobj->context.batch[i]->err = 0;
		*kobj->context.batch[i] = (rd_kafka_message_t)
		{
			.err = RD_KAFKA_RESP_ERR_NO_ERROR,
				.partition = -1,
				.payload = NULL,
				.len = 0,
				.key = NULL,
				.key_len = 0
		};
	}
	kobj->context.batch_size_consumed = kobj->context.inject_tuples_per_batch;
	kobj->context.batch_i = 0;
}
#endif

/**
 * Consume messages from Kafka using 'kobj'. The results are written into
 * 'kobj->context.batch'.
 */
static void
kafka_consume(KafkaObjects kobj)
{
	ssize_t		consume_result = rd_kafka_consume_batch_queue(
															  kobj->rkqu, kobj->context.request_timeout, kobj->context.batch, kobj->context.batch_size
	);

	/* Poll to handle stats callbacks */
	rd_kafka_poll(kobj->rk, 0);

	/* Timeout or a non-recoverable error */
	if (consume_result <= 0)
	{
		rd_kafka_resp_err_t err = rd_kafka_last_error();

		if (err == RD_KAFKA_RESP_ERR_NO_ERROR || err == ETIMEDOUT)
			ereport(NOTICE, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Kafka consume request returned 0 messages due to timeout. Consider increasing timeout to fetch data")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("Kafka-ADB: Failed to consume data from Kafka: %s [%d]", rd_kafka_err2str(err), err))
				);
	}
	kobj->context.batch_size_consumed = consume_result < 0 ? 0 : consume_result;
	kobj->context.batch_i = 0;

	elog(DEBUG1, "Kafka-ADB: Fetched %" PRId64 " messages", kobj->context.batch_size_consumed);
}

rd_kafka_message_t *
fetch_message(KafkaObjects kobj)
{
	Assert(PointerIsValid(kobj));
	Assert(PointerIsValid(kobj->context.batch));

	if (!kobj->context.request_made)
	{
		kobj->context.request_made = true;

#ifdef FAULT_INJECTOR
		if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
			kafka_consume_dummy(kobj);
		else
#endif
			kafka_consume(kobj);
	}

	rd_kafka_message_t *result = NULL;

	while (kobj->context.batch_i < kobj->context.batch_size_consumed)
	{
		rd_kafka_message_t *current = kobj->context.batch[kobj->context.batch_i++];

		if (current->err == RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			/*
			 * With 'kadb_fdw_inject_tuples', this case must always be taken.
			 * This way, we do not need to wrap 'rd_kafka_message_destroy()'
			 * calls below.
			 */
			result = current;
			break;
		}
		if (current->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
		{
			elog(DEBUG1, "Kafka-ADB: EOF message for partition %d is destroyed", current->partition);
			rd_kafka_message_destroy(current);
			continue;
		}
		/* else */
		DO_THEN_ERROR(
					  rd_kafka_message_destroy(current),
					  "Kafka-ADB: Errorneous message in batch: %s [%d]", rd_kafka_message_errstr(current), current->err
			);
	}

	return result;
}

List *
partition_list_kafka(List *options)
{
	List	   *volatile result = NIL;

	struct KafkaObjects kobj = KafkaObjectsEmptyStruct;

	kobj_initialize(&kobj, options);
	PG_TRY();
	{
		const struct rd_kafka_metadata *metadata;
		int			metadata_retrieval_attempts_done = 0;

		/* Repeat up to METADATA_RETRIEVAL_ATTEMPTS_MAX */
		while (true)
		{
			rd_kafka_resp_err_t err = rd_kafka_metadata(kobj.rk, 0, kobj.rkt, &metadata, defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS)));

			rd_kafka_poll(kobj.rk, 0);
			if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to retrieve metadata for topic '%s': %s [%d]", rd_kafka_topic_name(kobj.rkt), rd_kafka_err2str(err), (int) err)));
			}

			Assert(metadata->topic_cnt == 1);
			/* When rebalance is in progress, wait until it is available */
			if (
				metadata->topics[0].err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE &&
				(++metadata_retrieval_attempts_done) < METADATA_RETRIEVAL_ATTEMPTS_MAX
				)
			{
				ereport(NOTICE, (errcode(ERRCODE_FDW_ERROR), errmsg(
																	"Kafka-ADB: Failed to retrieve metadata for topic '%s' due to rebalance. Topic may be being created by Kafka. An attempt to retrieve metadata will be repeated in '%s'=%" PRId64 "ms",
																	rd_kafka_topic_name(kobj.rkt), KADB_SETTING_K_TIMEOUT_MS, defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS))
																	)));
				rd_kafka_metadata_destroy(metadata);
				pg_usleep(defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS)) * 1000);
				continue;
			}
			if (metadata->topics[0].err != RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				DO_THEN_ERROR(
							  rd_kafka_metadata_destroy(metadata),
							  "Kafka-ADB: Failed to retrieve metadata for topic '%s': %s [%d] (topic-specific error)", rd_kafka_topic_name(kobj.rkt), rd_kafka_err2str(metadata->topics[0].err), (int) metadata->topics[0].err
					);
			}
			break;
		}

		elog(DEBUG1, "Kafka-ADB: Retrieved %d partition(s) from topic '%s'", metadata->topics[0].partition_cnt, rd_kafka_topic_name(kobj.rkt));
		for (int p = 0; p < metadata->topics[0].partition_cnt; p++)
			result = lappend_int(result, metadata->topics[0].partitions[p].id);

		rd_kafka_metadata_destroy(metadata);
	}
	PG_CATCH();
	{
		kobj_destroy(&kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	kobj_destroy(&kobj, NULL, false);

	return result;
}

void
validate_partition_offset_pairs(List *options, List *partition_offset_pairs)
{
#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
		return;
#endif

	/* Automatic offsets imply offsets' increase is allowed */
	bool		offset_increase_allowed = defGetBoolean(get_option(options, KADB_SETTING_K_AUTOMATIC_OFFSETS));
	int			timeout = (int) defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS));
	const char *topic = defGetString(get_option(options, KADB_SETTING_K_TOPIC));

	struct KafkaObjects kobj = KafkaObjectsEmptyStruct;

	kobj_initialize(&kobj, options);
	PG_TRY();
	{
		ListCell   *it;

		foreach(it, partition_offset_pairs)
		{
			CHECK_FOR_INTERRUPTS();

			PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

			int64_t		offset_low;
			int64_t		offset_high;

			rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(kobj.rk, topic, pop->partition, &offset_low, &offset_high, timeout);

			switch (err)
			{
				case RD_KAFKA_RESP_ERR_NO_ERROR:
					break;
				case RD_KAFKA_RESP_ERR__TIMED_OUT:
					ereport(NOTICE, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to consume the oldest message from partition %d due to timeout. It is considered empty, offsets are not validated", pop->partition)));
					continue;
				default:
					ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to consume the oldest message from partition %d: %s [%d]", pop->partition, rd_kafka_err2str(err), err)));
			}

			elog(DEBUG1, "Kafka-ADB: Validate offsets for partition %d: local=%" PRId64 ", low=%" PRId64 ", high=%" PRId64, pop->partition, pop->offset, offset_low, offset_high);

			if (pop->offset < offset_low)
			{
				if (!offset_increase_allowed)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_ERROR),
							 errmsg("Kafka-ADB: Offset for partition %d (%" PRId64 ") is smaller than the smallest offset of an existing message in Kafka (%" PRId64 ")", pop->partition, pop->offset, offset_low))
						);

				ereport(NOTICE, (errmsg("Kafka-ADB: Offset for partition %d (%" PRId64 ") is increased to %" PRId64 " to match the lowest existing offset in Kafka", pop->partition, pop->offset, offset_low)));
				pop->offset = offset_low;
			}

			if (pop->offset > offset_high)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("Kafka-ADB: Offset for partition %d (%" PRId64 ") is bigger than offset of the next message to be inserted in Kafka (%" PRId64 ")", pop->partition, pop->offset, offset_high),
						 errdetail("Kafka might have crashed since the foreign table was created, and its offsets were reset."))
					);
			}
		}
	}
	PG_CATCH();
	{
		kobj_destroy(&kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	kobj_destroy(&kobj, NULL, false);
}

void
commit_offsets(List *options, List *partition_offset_pairs)
{
	int			partition_offset_pairs_l = list_length(partition_offset_pairs);

	if (partition_offset_pairs_l == 0)
		return;

	/* Form a representation of data to query for librdkafka */

	rd_kafka_topic_partition_list_t topic_partition_list = (rd_kafka_topic_partition_list_t) {
		.cnt = partition_offset_pairs_l,
		.size = sizeof(rd_kafka_topic_partition_t) * partition_offset_pairs_l,
		.elems = palloc(sizeof(rd_kafka_topic_partition_t) * partition_offset_pairs_l)
	};

	char	   *topic = defGetString(get_option(options, KADB_SETTING_K_TOPIC));
	ListCell   *it;
	size_t		i;

	foreach_with_count(it, partition_offset_pairs, i)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		topic_partition_list.elems[i] = (rd_kafka_topic_partition_t)
		{
			.topic = topic,
				.partition = pop->partition,
				.offset = pop->offset,

				.metadata = NULL,
				.metadata_size = 0,
				.opaque = NULL,
				.err = RD_KAFKA_RESP_ERR_NO_ERROR,
				._private = NULL
		};
	}

	/* Query Kafka */

	struct KafkaObjects kobj = KafkaObjectsEmptyStruct;

	kobj_initialize(&kobj, options);
	PG_TRY();
	{
		rd_kafka_resp_err_t err = rd_kafka_commit(kobj.rk, &topic_partition_list, false);

		if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to commit offsets: %s [%d]", rd_kafka_err2str(err), err)));
	}
	PG_CATCH();
	{
		kobj_destroy(&kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	kobj_destroy(&kobj, NULL, false);

	pfree(topic_partition_list.elems);
}

void
offsets_to_timestamp(List *options, List *partition_offset_pairs, int64_t timestamp_ms)
{
	int			partition_offset_pairs_l = list_length(partition_offset_pairs);

	if (partition_offset_pairs_l == 0)
		return;

	ListCell   *it;
	size_t		i;

	/* Form a representation of data to query for librdkafka */

	rd_kafka_topic_partition_list_t topic_partition_list = (rd_kafka_topic_partition_list_t) {
		.cnt = partition_offset_pairs_l,
		.size = sizeof(rd_kafka_topic_partition_t) * partition_offset_pairs_l,
		.elems = palloc(sizeof(rd_kafka_topic_partition_t) * partition_offset_pairs_l)
	};

	char	   *topic = defGetString(get_option(options, KADB_SETTING_K_TOPIC));

	foreach_with_count(it, partition_offset_pairs, i)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		topic_partition_list.elems[i] = (rd_kafka_topic_partition_t)
		{
			.topic = topic,
				.partition = pop->partition,
				.offset = timestamp_ms,

				.metadata = NULL,
				.metadata_size = 0,
				.opaque = NULL,
				.err = RD_KAFKA_RESP_ERR_NO_ERROR,
				._private = NULL
		};
	}

	/* Query Kafka */

	struct KafkaObjects kobj = KafkaObjectsEmptyStruct;

	bool		do_return_without_update = false;

	kobj_initialize(&kobj, options);
	PG_TRY();
	{
		rd_kafka_resp_err_t err = rd_kafka_offsets_for_times(kobj.rk, &topic_partition_list, (int) defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS)));

		if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
		{
			ereport(NOTICE, (errmsg("Kafka-ADB: No partitions from the offsets table exist in Kafka, offsets are left unchanged")));
			do_return_without_update = true;
		}
		else if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("Kafka-ADB: Failed to obtain offsets for timestamp from Kafka: %s [%d]", rd_kafka_err2str(err), err))
				);
		}
	}
	PG_CATCH();
	{
		kobj_destroy(&kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	kobj_destroy(&kobj, NULL, false);

	if (do_return_without_update)
	{
		pfree(topic_partition_list.elems);
		return;
	}

	/* Update the original partition_offset_pairs */

	foreach_with_count(it, partition_offset_pairs, i)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		Assert(pop->partition == topic_partition_list.elems[i].partition);

		if (
			topic_partition_list.elems[i].err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
			topic_partition_list.elems[i].offset == RD_KAFKA_OFFSET_INVALID
			)
		{
			ereport(NOTICE, (errmsg("Kafka-ADB: Partition %d does not exist in Kafka, offset (%" PRId64 ") is left unchanged", pop->partition, pop->offset)));
			continue;
		}
		if (
			topic_partition_list.elems[i].offset == RD_KAFKA_OFFSET_END
			)
		{
			ereport(NOTICE, (errmsg("Kafka-ADB: Partition %d is empty, offset (%" PRId64 ") is left unchanged", pop->partition, pop->offset)));
			continue;
		}
		if (topic_partition_list.elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: Failed to read offset at timestamp for partition %d: %s [%d]", pop->partition, rd_kafka_err2str(topic_partition_list.elems[i].err), topic_partition_list.elems[i].err)));
		}

		pop->offset = topic_partition_list.elems[i].offset;
	}

	pfree(topic_partition_list.elems);
}

/**
 * Query watermark offsets for all partitions in 'partition_offset_pairs'. The
 * results are saved in 'lower' and 'upper'.
 *
 * Lengths of 'partition_offset_pairs', 'lower', and 'upper' are guaranteed to
 * be the same after this function finishes successfully.
 *
 * Resulting 'lower' and 'upper' are lists of 'int64_t*'.
 *
 * This method manages Kafka connection internally.
 *
 * @param lower may be NULL
 * @param upper may be NULL
 */
static void
query_watermark_offsets(List *options, List *partition_offset_pairs, List **lower, List **upper)
{
	if (PointerIsValid(lower))
		*lower = NIL;
	if (PointerIsValid(upper))
		*upper = NIL;

	if (list_length(partition_offset_pairs) == 0)
		return;

	int			timeout = (int) defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS));
	const char *topic = defGetString(get_option(options, KADB_SETTING_K_TOPIC));

	struct KafkaObjects kobj = KafkaObjectsEmptyStruct;

	/*
	 * validate_partition_offset_pairs() is slightly different from this
	 * method: no errors are ignored here.
	 */
	kobj_initialize(&kobj, options);
	PG_TRY();
	{
		ListCell   *it;

		foreach(it, partition_offset_pairs)
		{
			CHECK_FOR_INTERRUPTS();

			PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);
			int64_t		low;
			int64_t		high;

			rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(kobj.rk, topic, pop->partition, &low, &high, timeout);

			if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
			{
				ereport(NOTICE,
						(errmsg("Partition %d does not exist in Kafka, offset (%" PRId64 ") is left unchanged", pop->partition, pop->offset))
					);
				low = pop->offset;
				high = pop->offset;
			}
			else if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("Kafka-ADB: Failed to obtain watermark offsets from Kafka: %s [%d]", rd_kafka_err2str(err), err))
					);
			}

			if (PointerIsValid(lower))
			{
				int64_t    *e = palloc(sizeof(int64_t));

				*e = low;
				*lower = lappend(*lower, e);
			}
			if (PointerIsValid(upper))
			{
				int64_t    *e = palloc(sizeof(int64_t));

				*e = high;
				*upper = lappend(*upper, e);
			}
		}
	}
	PG_CATCH();
	{
		kobj_destroy(&kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	kobj_destroy(&kobj, NULL, false);
}

/**
 * The implementation of 'offsets_to_earliest', and 'offsets_to_latest'. These
 * call the same method 'query_watermark_offsets' and process the result the
 * same way. The only difference is which argument they provide to the mentioned
 * method.
 *
 * @param which which argument to provide to 'query_watermark_offsets'. If -1,
 * "lower" is set to non-NULL; Otherwise, "upper" is set to non-NULL.
 */
static void
offsets_to_boundary(List *options, List *partition_offset_pairs, int which)
{
	List	   *results = NIL;

	if (which < 0)
		query_watermark_offsets(options, partition_offset_pairs, &results, NULL);
	else
		query_watermark_offsets(options, partition_offset_pairs, NULL, &results);

	ListCell   *it_partition_offset_pairs;
	ListCell   *it_results;

	forboth(it_partition_offset_pairs, partition_offset_pairs, it_results, results)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it_partition_offset_pairs);
		int64_t    *result = (int64_t *) lfirst(it_results);

		pop->offset = *result;

		pfree(result);
	}

	list_free(results);
}

void
offsets_to_earliest(List *options, List *partition_offset_pairs)
{
	offsets_to_boundary(options, partition_offset_pairs, -1);
}

void
offsets_to_latest(List *options, List *partition_offset_pairs)
{
	offsets_to_boundary(options, partition_offset_pairs, 1);
}

void
offsets_to_committed(List *options, List *partition_offset_pairs)
{
	int			partition_offset_pairs_l = list_length(partition_offset_pairs);

	if (partition_offset_pairs_l == 0)
		return;

	ListCell   *it;
	size_t		i;

	/* Form a representation of data to query for librdkafka */

	rd_kafka_topic_partition_list_t topic_partition_list = (rd_kafka_topic_partition_list_t) {
		.cnt = partition_offset_pairs_l,
		.size = sizeof(rd_kafka_topic_partition_t) * partition_offset_pairs_l,
		.elems = palloc(sizeof(rd_kafka_topic_partition_t) * partition_offset_pairs_l)
	};

	char	   *topic = defGetString(get_option(options, KADB_SETTING_K_TOPIC));

	foreach_with_count(it, partition_offset_pairs, i)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		topic_partition_list.elems[i] = (rd_kafka_topic_partition_t)
		{
			.topic = topic,
				.partition = pop->partition,
				.offset = RD_KAFKA_OFFSET_INVALID,

				.metadata = NULL,
				.metadata_size = 0,
				.opaque = NULL,
				.err = RD_KAFKA_RESP_ERR_NO_ERROR,
				._private = NULL
		};
	}

	/* Query Kafka */

	struct KafkaObjects kobj = KafkaObjectsEmptyStruct;

	kobj_initialize(&kobj, options);
	PG_TRY();
	{
		rd_kafka_resp_err_t err = rd_kafka_committed(kobj.rk, &topic_partition_list, (int) defGetInt64(get_option(options, KADB_SETTING_K_TIMEOUT_MS)));

		if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("Kafka-ADB: Failed to obtain committed offsets from Kafka: %s [%d]", rd_kafka_err2str(err), err))
				);
	}
	PG_CATCH();
	{
		kobj_destroy(&kobj, NULL, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	kobj_destroy(&kobj, NULL, false);

	/* Update the original partition_offset_pairs */

	foreach_with_count(it, partition_offset_pairs, i)
	{
		PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);

		Assert(pop->partition == topic_partition_list.elems[i].partition);

		if (
			topic_partition_list.elems[i].err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
			topic_partition_list.elems[i].offset == RD_KAFKA_OFFSET_INVALID
			)
		{
			ereport(NOTICE, (errmsg("Kafka-ADB: Partition %d does not exist in Kafka, offset (%" PRId64 ") is left unchanged", pop->partition, pop->offset)));
			continue;
		}
		if (topic_partition_list.elems[i].offset == RD_KAFKA_OFFSET_INVALID)
		{
			ereport(NOTICE, (errmsg("Kafka-ADB: Partition %d does not exist in Kafka, offset (%" PRId64 ") is left unchanged", pop->partition, pop->offset)));
			continue;
		}

		pop->offset = topic_partition_list.elems[i].offset;
	}

	pfree(topic_partition_list.elems);
}
