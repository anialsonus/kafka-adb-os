#include "settings.h"

#include <foreign/fdwapi.h>
#include <nodes/makefuncs.h>
#include <utils/faultinjector.h>

#include "deserialization/format.h"


#define STREQ(a, b) (strcmp(a, b) == 0)

#define ERROR_SETTING_REQUIRED(setting_name) ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED), errmsg("Kafka-ADB: '%s' is required", setting_name)))


/**
 * A list of all valid options' identifiers. Currently used only by
 * 'validate_options()'.
 *
 * NOTE: All new options must be added here.
 */
static const char *ValidOptions[] = {
	KADB_SETTING_K_BROKERS,
	KADB_SETTING_K_TOPIC,
	KADB_SETTING_K_CONSUMER_GROUP,
	KADB_SETTING_K_INITIAL_OFFSET,
	KADB_SETTING_K_ALLOW_OFFSET_INCREASE_HISTORICAL,
	KADB_SETTING_K_AUTOMATIC_OFFSETS,
	KADB_SETTING_K_SEG_BATCH,
	KADB_SETTING_K_TIMEOUT_MS,
	KADB_SETTING_K_SECURITY_PROTOCOL,

#ifdef FAULT_INJECTOR
	KADB_SETTING_K_TUPLES_PER_PARTITION_ON_INJECT,
#endif

	KADB_SETTING_KERBEROS_KEYTAB,
	KADB_SETTING_KERBEROS_PRINCIPAL,
	KADB_SETTING_KERBEROS_SERVICE_NAME,
	KADB_SETTING_KERBEROS_MIN_TIME_BEFORE_RELOGIN,

	KADB_SETTING_FORMAT,

	KADB_SETTING_AVRO_SCHEMA,
	KADB_SETTING_AVRO_SCHEMA_HISTORICAL,

	KADB_SETTING_CSV_QUOTE,
	KADB_SETTING_CSV_DELIMITER,
	KADB_SETTING_CSV_NULL_STRING,
	KADB_SETTING_CSV_IGNORE_HEADER,
	KADB_SETTING_CSV_ATTRIBUTE_TRIM_WHITESPACE,

#ifdef FAULT_INJECTOR
	KADB_SETTING_CSV_DATA_ON_INJECT,
#endif
#ifdef FAULT_INJECTOR
	KADB_SETTING_TEXT_DATA_ON_INJECT,
#endif

	KADB_SETTING__PARTITION_DISTRIBUTION,
	KADB_SETTING__PARTITIONS_ABSENT,
	KADB_SETTING__DISTRIBUTED_TABLE
};


/**
 * Check FOREIGN TABLE execution location: ensure it is 'all segments'.
 *
 * We could drop this requirement, but then all data from Kafka would flow
 * through master.
 */
static void
validate_mpp_execute(Oid foreigntableid)
{
	ForeignTable *table;

	table = GetForeignTable(foreigntableid);

	if (table->exec_location != FTEXECLOCATION_ALL_SEGMENTS)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Kafka-ADB: Only mpp_execute 'all segments' foreign table OPTION is currently supported")));
}

/**
 * Retrieve all options for the given 'foreigntableid'
 */
static List *
retrieve_options(Oid foreigntableid)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;

	List	   *result;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	result = NIL;
	result = list_concat(result, table->options);
	result = list_concat(result, server->options);
	result = list_concat(result, wrapper->options);

	return result;
}

/**
 * Collate options, so that the following guarantees are true for 'options'
 * after this call:
 * 1. For each DefElem key, such key is present exactly once in 'options'
 * 2. FIRST option wins: if 'options' provided to the function contains multiple
 * instances of a DefElem key, only the first instance is left as a result of
 * this function.
 *
 * Current implementation of this method provides these guarantees ONLY in case
 * 'get_option()' method is used to retrieve a DefElem.
 */
static void
collate_options(List **options)
{
	/*
	 * get_option() walks the list from start to end and returns the first
	 * value found. This behaviour provides the guarantee required to be
	 * fulfilled by this function, without any List modifications!
	 *
	 * In the future, we may change this behaviour.
	 */
}

/**
 * Replace DefElem 'historical_option', if present, with DefElem
 * 'current_option', appended to 'options'. 'historical_option' DefElems are
 * left untouched.
 *
 * If multiple 'historical_option's are present, the first one is prioritized
 * (and it's value is the only one which is appended to 'options').
 */
static void
historical_option_to_current(List **options, char *historical_option, char *current_option)
{
	AssertArg(PointerIsValid(options));

	ListCell   *it;

	foreach(it, *options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (STREQ(key, historical_option))
		{
			ereport(WARNING, (errcode(ERRCODE_WARNING_DEPRECATED_FEATURE), errmsg("Kafka-ADB: '%s' OPTION is deprecated; use '%s' instead", historical_option, current_option)));
			*options = lappend(*options, makeDefElem(current_option, (Node *) makeString(defGetString(option))));
			break;
		}
	}
}

/**
 * Convert historical options in 'options' to current ones.
 */
static void
convert_historical_options(List **options)
{
	AssertArg(PointerIsValid(options));

	historical_option_to_current(options, KADB_SETTING_AVRO_SCHEMA_HISTORICAL, KADB_SETTING_AVRO_SCHEMA);
	historical_option_to_current(options, KADB_SETTING_K_ALLOW_OFFSET_INCREASE_HISTORICAL, KADB_SETTING_K_AUTOMATIC_OFFSETS);
}

/**
 * Convert the given 'node' from a 'Value' of type 'String' to a 'Value' of type
 * 'Integer'.
 *
 * If the number cannot be parsed, reports this by 'elog'.
 *
 * @param option_name a name to use in error message
 */
static void
def_string_to_int64(Node **node, const char *option_name)
{
	if (!PointerIsValid(strVal(*node)) || strlen(strVal(*node)) < 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Kafka-ADB: '%s' OPTION must be an integer", option_name)));

	int64		result = strtol(strVal(*node), NULL, 10);

	if (errno == EINVAL || errno == ERANGE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Kafka-ADB: '%s' OPTION must be an integer", option_name)));

	pfree(*node);
	*node = (Node *) makeInteger(result);
}

/**
 * Parse (change types, if necessary) and validate authentication settings in
 * 'options'.
 *
 * @param check_required 'true' if the completeness of the set of 'options'
 * must be checked, i.e. that all required options are provided.
 */
static void
parse_authentication_options(List *options, bool check_required)
{
	bool		provided_k_security_protocol = false;
	bool		provided_kerberos_keytab = false;
	bool		provided_kerberos_principal = false;
	bool		provided_kerberos_service_name = false;
	bool		provided_kerberos_min_time_before_relogin = false;

	ListCell   *it;

	foreach(it, options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (STREQ(key, KADB_SETTING_K_SECURITY_PROTOCOL))
		{
			provided_k_security_protocol = true;
		}
		else if (STREQ(key, KADB_SETTING_KERBEROS_KEYTAB))
		{
			provided_kerberos_keytab = true;
		}
		else if (STREQ(key, KADB_SETTING_KERBEROS_PRINCIPAL))
		{
			provided_kerberos_principal = true;
		}
		else if (STREQ(key, KADB_SETTING_KERBEROS_SERVICE_NAME))
		{
			provided_kerberos_service_name = true;
		}
		else if (STREQ(key, KADB_SETTING_KERBEROS_MIN_TIME_BEFORE_RELOGIN))
		{
			provided_kerberos_min_time_before_relogin = true;
		}
	}

	/*
	 * Kerberos authentication settings are expected to be provided in a
	 * single object. Thus, they must form a complete set even when
	 * 'check_required' is 'false'.
	 */

	if (
		!provided_kerberos_keytab && (
									  provided_kerberos_principal ||
									  provided_kerberos_service_name ||
									provided_kerberos_min_time_before_relogin
									  )
		)
		ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED), errmsg("Kafka-ADB: '%s' OPTION is required to enable Kerberos authentication", KADB_SETTING_KERBEROS_KEYTAB)));

	if (provided_kerberos_keytab && !provided_k_security_protocol)
		ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED), errmsg("Kafka-ADB: '%s' OPTION is required to enable Kerberos authentication", KADB_SETTING_K_SECURITY_PROTOCOL)));
}

/**
 * Parse (change types, if necessary) and validate settings for CSV
 * deserialization format.
 */
static void
parse_csv_options(List *options)
{
	ListCell   *it;

	foreach(it, options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (
			STREQ(key, KADB_SETTING_CSV_DELIMITER)
			|| STREQ(key, KADB_SETTING_CSV_QUOTE)
			)
		{
			char	   *value = defGetString(option);

			if (strnlen(value, 2) != 1)
			{
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Kafka-ADB: '%s' OPTION must be a single character", key)));
			}
		}
		else if (STREQ(key, KADB_SETTING_CSV_IGNORE_HEADER))
		{
			defGetBoolean(option);
		}
		else if (STREQ(key, KADB_SETTING_CSV_ATTRIBUTE_TRIM_WHITESPACE))
		{
			defGetBoolean(option);
		}
	}
}

#ifdef FAULT_INJECTOR
/**
 * Parse (change types, if necessary) and validate fault injector settings in
 * 'options'.
 *
 * @param check_required 'true' if the completeness of the set of 'options'
 * must be checked, i.e. that all required options are provided.
 */
static void
parse_inject_options(List *options, bool check_required)
{
	bool		provided_tuples_per_partition_on_inject = false;

	ListCell   *it;

	foreach(it, options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (STREQ(key, KADB_SETTING_K_TUPLES_PER_PARTITION_ON_INJECT))
		{
			def_string_to_int64(&option->arg, key);
			provided_tuples_per_partition_on_inject = true;
		}
	}

	if (check_required)
	{
		if (!provided_tuples_per_partition_on_inject)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_TUPLES_PER_PARTITION_ON_INJECT);
	}
}

/**
 * Parse (change types, if necessary) and validate CSV data injection settings
 * for testing.
 *
 * @param check_required 'true' if the completeness of the set of 'options'
 * must be checked, i.e. that all required options are provided.
 */
static void
parse_inject_csv_options(List *options, bool check_required)
{
	bool		provided_csv_data_on_inject = false;

	ListCell   *it;

	foreach(it, options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (STREQ(key, KADB_SETTING_CSV_DATA_ON_INJECT))
		{
			provided_csv_data_on_inject = true;
		}
	}

	if (check_required)
	{
		if (!provided_csv_data_on_inject)
			ERROR_SETTING_REQUIRED(KADB_SETTING_CSV_DATA_ON_INJECT);
	}
}

static void
parse_inject_text_options(List *options, bool check_required)
{
	bool		provided_text_data_on_inject = false;

	ListCell   *it;

	foreach(it, options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (STREQ(key, KADB_SETTING_TEXT_DATA_ON_INJECT))
		{
			provided_text_data_on_inject = true;
		}
	}

	if (check_required)
	{
		if (!provided_text_data_on_inject)
			ERROR_SETTING_REQUIRED(KADB_SETTING_TEXT_DATA_ON_INJECT);
	}
}
#endif

/**
 * Parse (change types, if necessary) and validate the given list of 'options'.
 *
 * @param check_required 'true' if the completeness of the set of 'options'
 * must be checked, i.e. that all required options are provided.
 */
static void
parse_options(List *options, bool check_required)
{
	bool		provided_k_brokers = false;
	bool		provided_k_topic = false;
	bool		provided_k_consumer_group = false;
	bool		provided_k_initial_offset = false;
	bool		provided_k_seg_batch = false;
	bool		provided_k_timeout_ms = false;
	bool		provided_k_automatic_offsets = false;
	bool		provided_format = false;

	ListCell   *it;

	foreach(it, options)
	{
		DefElem    *option = (DefElem *) lfirst(it);
		const char *key = option->defname;

		if (STREQ(key, KADB_SETTING_K_BROKERS))
		{
			provided_k_brokers = true;
		}
		else if (STREQ(key, KADB_SETTING_K_TOPIC))
		{
			provided_k_topic = true;
		}
		else if (STREQ(key, KADB_SETTING_K_CONSUMER_GROUP))
		{
			provided_k_consumer_group = true;
		}
		else if (STREQ(key, KADB_SETTING_K_INITIAL_OFFSET))
		{
			provided_k_initial_offset = true;
			def_string_to_int64(&option->arg, key);
		}
		else if (STREQ(key, KADB_SETTING_K_AUTOMATIC_OFFSETS))
		{
			provided_k_automatic_offsets = true;
			defGetBoolean(option);
		}
		else if (STREQ(key, KADB_SETTING_K_SEG_BATCH))
		{
			provided_k_seg_batch = true;
			def_string_to_int64(&option->arg, key);
			if (defGetInt64(option) < 1)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Kafka-ADB: '%s' OPTION must be a positive integer value", key)));
		}
		else if (STREQ(key, KADB_SETTING_K_TIMEOUT_MS))
		{
			provided_k_timeout_ms = true;
			def_string_to_int64(&option->arg, key);
		}
		else if (STREQ(key, KADB_SETTING_FORMAT))
		{
			provided_format = true;
			if (resolve_deserialization_format(defGetString(option)) == DESERIALIZATION_FORMAT_INVALID)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Kafka-ADB: '%s' OPTION is set to unknown value '%s'", key, strVal(option->arg))));
		}
	}

	if (check_required)
	{
		if (!provided_k_brokers)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_BROKERS);
		if (!provided_k_topic)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_TOPIC);
		if (!provided_k_consumer_group)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_CONSUMER_GROUP);
		if (!provided_k_initial_offset)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_INITIAL_OFFSET);
		if (!provided_k_automatic_offsets)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_AUTOMATIC_OFFSETS);
		if (!provided_k_seg_batch)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_SEG_BATCH);
		if (!provided_k_timeout_ms)
			ERROR_SETTING_REQUIRED(KADB_SETTING_K_TIMEOUT_MS);
		if (!provided_format)
			ERROR_SETTING_REQUIRED(KADB_SETTING_FORMAT);
	}

	/*
	 * This check is required because validator may also call this function.
	 * However, it usually has incomplete set of OPTIONs: KADB_SETTING_FORMAT
	 * may be absent.
	 */
	if (PointerIsValid(get_option(options, KADB_SETTING_FORMAT)))
	{
		switch (resolve_deserialization_format(defGetString(get_option(options, KADB_SETTING_FORMAT))))
		{
			case AVRO:
				break;
			case CSV:
				parse_csv_options(options);
				break;
			case TEXT:
				break;
			default:
				Assert(false);
		}
	}

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
		parse_inject_options(options, check_required);
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_csv") == FaultInjectorTypeSkip)
		parse_inject_csv_options(options, check_required);
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_text") == FaultInjectorTypeSkip)
		parse_inject_text_options(options, check_required);
#endif

	parse_authentication_options(options, check_required);
}

/**
 * Report unknown options by 'ereport(WARNING)'.
 *
 * This method requires a complete set of supported options in
 */
static void
report_unknown_options(List *options)
{
	ListCell   *it;

	foreach(it, options)
	{
		const char *option = ((DefElem *) lfirst(it))->defname;
		size_t		i;
		bool		option_found = false;

		for (i = 0; i < sizeof(ValidOptions) / sizeof(ValidOptions[0]); i++)
		{
			if (STREQ(option, ValidOptions[i]))
			{
				option_found = true;
				break;
			}
		}
		if (!option_found)
		{
			ereport(WARNING,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("Kafka-ADB: Unknown OPTION '%s'", option))
				);
		}
	}
}

List *
get_and_validate_options(Oid foreigntableid)
{
	validate_mpp_execute(foreigntableid);

	List	   *result = retrieve_options(foreigntableid);

	validate_options(&result, true);

	return result;
}

DefElem *
get_option(List *options, const char *key)
{
	ListCell   *it;

	foreach(it, options)
	{
		if (STREQ(((DefElem *) lfirst(it))->defname, key))
		{
			return (DefElem *) lfirst(it);
		}
	}

	return NULL;
}

void
validate_options(List **options, bool check_required)
{
	collate_options(options);
	convert_historical_options(options);
	report_unknown_options(*options);
	parse_options(*options, check_required);
}
