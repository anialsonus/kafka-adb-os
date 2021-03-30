#ifndef KADB_FDW_KAFKA_CONSUMER_INCLUDED
#define KADB_FDW_KAFKA_CONSUMER_INCLUDED

/*
 * Kafka consumer implementation.
 *
 * A concept of "atomicity" is used in this source file, as well as in other
 * sources. It denotes only a simple contract: multiple objects (e.g.
 * partitions) are given to a lirdkafka function as a whole, not one-by-one. See
 * examples below.
 *
 * This implies some methods described as "atomic" may actually be non-atomic,
 * depending on librdkafka implementation.
 */

#include <postgres.h>

#include <librdkafka/rdkafka.h>

#include <nodes/pg_list.h>

#include "offsets.h"


/**
 * A context of a request to Kafka.
 */
typedef struct KafkaRequestContext
{
	bool		request_made;	/* Whether a request to Kafka has already been
								 * made */
	int32_t		request_timeout;	/* Request timeout */

	rd_kafka_message_t **batch; /* The batch */
	ssize_t		batch_size;		/* Total size of 'batch' */
	ssize_t		batch_size_consumed;	/* Number of received items in 'batch' */
	ssize_t		batch_i;		/* Current position in 'batch' */

#ifdef FAULT_INJECTOR
	ssize_t		inject_tuples_per_batch;		/* Number of injected tuples
												 * per batch */
#endif
}	KafkaRequestContext;

/**
 * Obscure librdkafka state.
 */
typedef struct KafkaObjects *KafkaObjects;


/**
 * Initialize a connection to Kafka using librdkafka, create a topic and
 * subscribe to a set of partitions given by 'partition_offset_pairs'.
 *
 * If an error happens, it is logged by 'elog()'.
 *
 * @param kobj_ptr a pointer to 'KafkaObjects', where the result is placed to
 * @param options a list of 'DefElem's - FOREIGN TABLE options
 */
void		kobj_initialize_topic_connection(KafkaObjects * kobj_ptr, List *options, List *partition_offset_pairs);

/**
 * Destroy a librdkafka connection, represented by 'kobj'.
 */
void		kobj_finish_and_destroy(KafkaObjects kobj, List *partition_offset_pairs);

/**
 * Restart a Kafka pipeline. Internally, this method only restarts the queue.
 */
void		kobj_restart(KafkaObjects kobj, List *partition_offset_pairs);

/**
 * Fetch a message from Kafka.
 *
 * This method uses a buffer in 'KafkaRequestContext' to store retrieved
 * messages. At the first call, however, it makes a request to Kafka to fetch
 * messages from the queue created by 'kobj_initialize_topic_connection()'.
 *
 * @return NULL if all requested messages have been read. The messages returned
 * are GUARANTEED to have no errors. Errorneous messages are reported by
 * 'elog()' from inside this method.
 *
 * @note the returned message MUST be freed with 'rd_kafka_message_destroy()',
 * except when tuple injection is enabled.
 */
rd_kafka_message_t *fetch_message(KafkaObjects kobj);

/**
 * Retrieve a list of partitions from Kafka.
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 *
 * @return NOT atomic result: a list of Int
 */
List	   *partition_list_kafka(List *options);

/**
 * Validate the given list of 'PartitionOffsetPair's, ensuring the given pairs
 * contain offsets which are present in Kafka, and do not violate certain
 * constraints.
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 * @param partition_offset_pairs are processed NOT atomically
 */
void		validate_partition_offset_pairs(List *options, List *partition_offset_pairs);

/**
 * Commit the given 'partition_offset_pairs' to Kafka.
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 * @param partition_offset_pairs are processed ATOMICALLY
 */
void		commit_offsets(List *options, List *partition_offset_pairs);

/**
 * Synchronize the given 'partition_offset_pairs' with offsets stored in Kafka.
 *
 * The resulting offsets are the earliest offsets whose timestamps are greater
 * or equal to the given 'timestamp_ms'.
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 * @param partition_offset_pairs are processed ATOMICALLY
 * @param timestamp_ms timestamp in milliseconds since the UNIX Epoch, UTC
 */
void		offsets_to_timestamp(List *options, List *partition_offset_pairs, int64_t timestamp_ms);

/**
 * Synchronize the given 'partition_offset_pairs' with offsets stored in Kafka.
 *
 * The resulting offsets are the earliest offsets present in Kafka.
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 * @param partition_offset_pairs are processed NOT atomically
 */
void		offsets_to_earliest(List *options, List *partition_offset_pairs);

/**
 * Synchronize the given 'partition_offset_pairs' with offsets stored in Kafka.
 *
 * The resulting offsets are the latest offsets present in Kafka. If no new
 * messages are added to Kafka between this call and a subsequent SELECT, the
 * latter returns an empty result set (zero tuples).
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 * @param partition_offset_pairs are processed NOT atomically
 */
void		offsets_to_latest(List *options, List *partition_offset_pairs);

/**
 * Synchronize the given 'partition_offset_pairs' with offsets stored in Kafka.
 *
 * The resulting offsets are set to the "stored offsets" known to Kafka.
 *
 * This method manages Kafka connection internally.
 *
 * @param options FOREIGN TABLE options
 * @param partition_offset_pairs are processed ATOMICALLY
 */
void		offsets_to_committed(List *options, List *partition_offset_pairs);


#endif   /* KADB_FDW_KAFKA_CONSUMER_INCLUDED */
