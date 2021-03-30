#ifndef KADB_FDW_SETTINGS_INCLUDED
#define KADB_FDW_SETTINGS_INCLUDED

/*
 * Kafka-ADB FDW settings definitions.
 */

#include <postgres.h>

#include <commands/defrem.h>
#include <nodes/parsenodes.h>
#include <nodes/pg_list.h>


/* A comma-separated list of Kafka brokers */
#define KADB_SETTING_K_BROKERS "k_brokers"
/* Kafka topic name */
#define KADB_SETTING_K_TOPIC "k_topic"
/* Kafka consumer group name */
#define KADB_SETTING_K_CONSUMER_GROUP "k_consumer_group"
/* Default offset (for partitions not present in the offsets table) */
#define KADB_SETTING_K_INITIAL_OFFSET "k_initial_offset"
/*
 * Allow to increase offset when minimum Kafka offset is bigger
 * than the one in global offsets' table.
 * A historical setting; replaced by KADB_SETTING_K_AUTOMATIC_OFFSETS
 */
#define KADB_SETTING_K_ALLOW_OFFSET_INCREASE_HISTORICAL "k_allow_offset_increase"
/* Manage offsets automatically */
#define KADB_SETTING_K_AUTOMATIC_OFFSETS "k_automatic_offsets"
/* Maximum Kafka messages retrieved by one segment in a single SELECT */
#define KADB_SETTING_K_SEG_BATCH "k_seg_batch"
/* Kafka request timeout */
#define KADB_SETTING_K_TIMEOUT_MS "k_timeout_ms"
/* Security protocol to use with Kafka (supported values: 'sasl_plaintext', 'sasl_ssl') */
#define KADB_SETTING_K_SECURITY_PROTOCOL "k_security_protocol"

#ifdef FAULT_INJECTOR
/* Number of tuples for fault injector to return for each partition */
#define KADB_SETTING_K_TUPLES_PER_PARTITION_ON_INJECT "k_tuples_per_partition_on_inject"
#endif

/*
 * Kerberos keytab file location. If this setting is given, Kerberos
 * authentication is assumed.
 */
#define KADB_SETTING_KERBEROS_KEYTAB "kerberos_keytab"
/* Kerberos principal */
#define KADB_SETTING_KERBEROS_PRINCIPAL "kerberos_principal"
/* Kerberos service name */
#define KADB_SETTING_KERBEROS_SERVICE_NAME "kerberos_service_name"
/* Kerberos minimum relogin time */
#define KADB_SETTING_KERBEROS_MIN_TIME_BEFORE_RELOGIN "kerberos_min_time_before_relogin"

/* Serialization format */
#define KADB_SETTING_FORMAT "format"

/* AVRO: Explicit schema to parse incoming data */
#define KADB_SETTING_AVRO_SCHEMA "avro_schema"
/* AVRO: Historical name for KADB_SETTING_AVRO_SCHEMA */
#define KADB_SETTING_AVRO_SCHEMA_HISTORICAL "schema"

/* CSV: Quote character */
#define KADB_SETTING_CSV_QUOTE "csv_quote"
/* CSV: Delimiter character */
#define KADB_SETTING_CSV_DELIMITER "csv_delimeter"
/* CSV: A string denoting NULLs */
#define KADB_SETTING_CSV_NULL_STRING "csv_null"
/* CSV: Ignore the first line (before delimeter) of the incoming message */
#define KADB_SETTING_CSV_IGNORE_HEADER "csv_ignore_header"
/* CSV: Trim trailing whitespace at start and end of each attribute */
#define KADB_SETTING_CSV_ATTRIBUTE_TRIM_WHITESPACE "csv_attribute_trim_whitespace"

#ifdef FAULT_INJECTOR
/* A string to inject as parser input (for each "message") to test 'csv' format */
#define KADB_SETTING_CSV_DATA_ON_INJECT "csv_data_on_inject"
#endif

#ifdef FAULT_INJECTOR
/* A string to inject as parser input (for each "message") to test 'text' format */
#define KADB_SETTING_TEXT_DATA_ON_INJECT "text_data_on_inject"
#endif

/* Distribution of partitions across segments. Internal option */
#define KADB_SETTING__PARTITION_DISTRIBUTION "_partition_distribution"
/* Partitions absent in the offsets table. Internal option */
#define KADB_SETTING__PARTITIONS_ABSENT "_partitions_absent"
/* Distributed table name. Internal option */
#define KADB_SETTING__DISTRIBUTED_TABLE "_distributed_table"


/**
 * Get all options for a foreign table with the given 'foreigntableid'.
 *
 * @return a list of options. It is guaranteed to have no more than one mapping
 * for each key, and no unknown keys.
 */
List	   *get_and_validate_options(Oid foreigntableid);

/**
 * Get an option by key
 *
 * @return NULL if an option is not found
 */
DefElem    *get_option(List *options, const char *key);

/**
 * Validate 'options'. Check all required options are present if
 * 'check_required' is 'true'.
 *
 * Invalid options are reported by 'ereport(ERROR)'.
 * Unknown options are reported by 'ereport(WARNING)'.
 */
void		validate_options(List **options, bool check_required);


#endif   /* KADB_FDW_SETTINGS_INCLUDED */
