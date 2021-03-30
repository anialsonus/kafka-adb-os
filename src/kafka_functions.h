#ifndef KADB_FDW_KAFKA_FUNCTIONS_INCLUDED
#define KADB_FDW_KAFKA_FUNCTIONS_INCLUDED

/*
 * Functions for interactions with Kafka involving global offsets' table data.
 */

#include <postgres.h>

#include <funcapi.h>


/**
 * Commit offsets stored in the global offsets' table for the given 'ftoid'
 * to Kafka.
 *
 * This method works ATOMICALLY.
 *
 * @param ftoid OID of a FOREIGN TABLE
 */
void		ft_commit_offsets(Oid ftoid);

/**
 * Load offsets from Kafka for the given 'ftoid'.
 *
 * The resulting offsets are the earliest offsets whose timestamps are greater
 * or equal to the given 'timestamp_ms' stored in Kafka.
 *
 * This method is ATOMIC.
 *
 * @param ftoid OID of a FOREIGN TABLE
 * @param timestamp_ms timestamp in milliseconds since the UNIX Epoch, UTC
 *
 * @return a list of 'PartitionOffsetPair *'
 */
List	   *ft_load_offsets_at_timestamp(Oid ftoid, int64_t timestamp_ms);

/**
 * Load offsets from Kafka for the given 'ftoid'.
 *
 * The resulting offsets are the earliest offsets present in Kafka.
 *
 * This method is NOT atomic.
 *
 * @param ftoid OID of a FOREIGN TABLE
 *
 * @return a list of 'PartitionOffsetPair *'
 */
List	   *ft_load_offsets_earliest(Oid ftoid);

/**
 * Load offsets from Kafka for the given 'ftoid'.
 *
 * The resulting offsets are the latest offsets present in Kafka. If no new
 * messages are added to Kafka between this call and a subsequent SELECT, the
 * latter returns an empty result set (zero tuples).
 *
 * This method is NOT atomic.
 *
 * @param ftoid OID of a FOREIGN TABLE
 *
 * @return a list of 'PartitionOffsetPair *'
 */
List	   *ft_load_offsets_latest(Oid ftoid);

/**
 * Load offsets from Kafka for the given 'ftoid'.
 *
 * The resulting offsets are set to the "stored offsets" known to Kafka.
 *
 * This method is ATOMIC.
 *
 * @param ftoid OID of a FOREIGN TABLE
 *
 * @return a list of 'PartitionOffsetPair *'
 */
List	   *ft_load_offsets_committed(Oid ftoid);

/**
 * Load a list of partitions available in Kafka.
 *
 * This method is NOT atomic.
 *
 * @param ftoid OID of a FOREIGN TABLE
 *
 * @return a list of 'PartitionOffsetPair *'. Note this method does NOT query
 * offsets from Kafka; offset for new partitions (not present in the global
 * offsets' table) is set to 'KADB_SETTING_K_INITIAL_OFFSET'.
 */
List	   *ft_load_partitions(Oid ftoid);


#endif   /* KADB_FDW_KAFKA_FUNCTIONS_INCLUDED */
