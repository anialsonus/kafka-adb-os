#include "csv_deserializer.h"

#include <stdlib.h>

#include <csv.h>

#include <access/htup.h>
#include <utils/memutils.h>
#include <utils/faultinjector.h>

#include "settings.h"
#include "deserialization/attribute_postgres.h"


/* Deserialization directives obtained from 'options' */
typedef struct CSVDeserializationSettings
{
	/* Ignore the first tuple in each pass of deserialization */
	bool		ignore_header;
	/* Do not form HeapTuple from the next tuple */
	bool		ignore_one_tuple;
	/* String denoting NULL */
	char	   *null_string;
}	CSVDeserializationSettings;

/**
 * Data used by the deserialization process on-the-fly.
 */
typedef struct CSVDeserializationMetadata
{
	/* Deserialization instructions for each attribute */
	AttributeDeserializationInfo *adis;
	/* "Iterator" over 'adis' */
	size_t		adis_i;
	/* Datums for the CURRENT record */
	Datum	   *datums;
	/* Null mask for the CURRENT record */
	bool	   *nulls;
	/* Tuple descriptor */
	TupleDesc	tupledesc;
	/* Settings altering the deserialization process */
	CSVDeserializationSettings settings;
	/* RESULTING tuples */
	List	   *result;
#ifdef FAULT_INJECTOR
	/* Raw data to use for tests */
	char	   *data;
	size_t		data_l;
#endif
}	CSVDeserializationMetadata;

/* See definition in the header */
typedef struct CSVDeserializationStateObject
{
	CSVDeserializationMetadata *metadata;
	struct csv_parser *parser;
}	CSVDeserializationStateObject;


/**
 * Set 'settings' using values from 'options'.
 */
static void
set_deserialization_settings(CSVDeserializationSettings * settings, List *options)
{
	settings->null_string = NULL;
	if (PointerIsValid(get_option(options, KADB_SETTING_CSV_NULL_STRING)))
	{
		char	   *null_string = defGetString(get_option(options, KADB_SETTING_CSV_NULL_STRING));

		settings->null_string = palloc(strlen(null_string) + 1);
		strcpy(settings->null_string, null_string);
	}

	settings->ignore_header = PointerIsValid(get_option(options, KADB_SETTING_CSV_IGNORE_HEADER)) ?
		defGetBoolean(get_option(options, KADB_SETTING_CSV_IGNORE_HEADER)) :
		false;
}

/**
 * Form an instance of 'CSVDeserializationMetadata' using 'tupledesc'.
 */
static CSVDeserializationMetadata *
make_csv_deserialization_metadata(TupleDesc tupledesc, List *options)
{
	CSVDeserializationMetadata *result = palloc(sizeof(CSVDeserializationMetadata));

	result->adis = (AttributeDeserializationInfo *) palloc(sizeof(AttributeDeserializationInfo) * tupledesc->natts);
	for (int i = 0; i < tupledesc->natts; i++)
	{
		fill_attribute_deserialization_info(&result->adis[i], tupledesc, i);
	}

	result->adis_i = 0;

	result->datums = palloc(sizeof(Datum) * tupledesc->natts);
	result->nulls = palloc(sizeof(bool) * tupledesc->natts);

	result->tupledesc = tupledesc;

	result->result = NIL;

	set_deserialization_settings(&result->settings, options);

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_csv") == FaultInjectorTypeSkip)
	{
		char	   *inject_data = defGetString(get_option(options, KADB_SETTING_CSV_DATA_ON_INJECT));

		result->data_l = strlen(inject_data) + 1;
		result->data = palloc(result->data_l);
		strcpy(result->data, inject_data);
	}
#endif

	return result;
}

/**
 * 'realloc()' implementation for libcsv csv_parser that uses correct memory
 * context for allocation.
 */
static void *
csv_postgres_realloc(void *ptr, size_t size)
{
	void	   *volatile result = NULL;

	MemoryContext oldcontext = MemoryContextSwitchTo(PortalContext);

	PG_TRY();
	{
		do
		{
			/* free */
			if (size == 0)
			{
				if (PointerIsValid(ptr))
					pfree(ptr);
				break;
			}
			/* malloc */
			if (!PointerIsValid(ptr))
			{
				result = palloc(size);
				break;
			}
			/* realloc */
			result = repalloc(ptr, size);
		}
		while (false);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);

	return result;
}

/**
 * A method that returns 'false' for any input.
 *
 * Currently used to prevent libcsv from trimming whitespace.
 */
static int
ignore_character_func(unsigned char c)
{
	return (int) false;
}

/**
 * Allocate and prepare 'struct csv_parser' according to the given 'options'.
 */
static struct csv_parser *
make_prepare_csv_parser(List *options)
{
	struct csv_parser *result = palloc(sizeof(struct csv_parser));

	csv_init(result, (unsigned char) (CSV_APPEND_NULL | CSV_EMPTY_IS_NULL));

	csv_set_realloc_func(result, csv_postgres_realloc);
	csv_set_free_func(result, pfree);

	/* Validity of each option MUST be checked at planning stage. */
	if (PointerIsValid(get_option(options, KADB_SETTING_CSV_QUOTE)))
		csv_set_quote(result, (unsigned char) defGetString(get_option(options, KADB_SETTING_CSV_QUOTE))[0]);
	if (PointerIsValid(get_option(options, KADB_SETTING_CSV_DELIMITER)))
		csv_set_delim(result, (unsigned char) defGetString(get_option(options, KADB_SETTING_CSV_DELIMITER))[0]);
	if (
		PointerIsValid(get_option(options, KADB_SETTING_CSV_ATTRIBUTE_TRIM_WHITESPACE))
		&& false == defGetBoolean(get_option(options, KADB_SETTING_CSV_ATTRIBUTE_TRIM_WHITESPACE))
		)
		csv_set_space_func(result, ignore_character_func);

	return result;
}

CSVDeserializationState
prepare_deserialization_csv(TupleDesc tupledesc, List *options)
{
	CSVDeserializationState result = palloc(sizeof(CSVDeserializationStateObject));

	result->metadata = make_csv_deserialization_metadata(tupledesc, options);
	result->parser = make_prepare_csv_parser(options);

	return result;
}

/**
 * A callback called by libcsv when a field is parsed.
 */
static void
csv_field_callback(void *value, size_t value_l, void *metadata_ptr)
{
	CSVDeserializationMetadata *metadata = (CSVDeserializationMetadata *) metadata_ptr;

	if (metadata->adis_i >= (size_t) metadata->tupledesc->natts)
		elog(ERROR, "Kafka-ADB: CSV contains more fields than there are attributes in the FOREIGN TABLE");

	if (metadata->settings.ignore_one_tuple)
		return;

	/* Parser MUST null-terminate 'value', so this cast is safe */
	char	   *value_str = (char *) value;

	/* Various NULL conditions */
	if (
		metadata->adis[metadata->adis_i].is_dropped ||
		!PointerIsValid(value) ||
		strlen(value_str) == 0
		)
	{
		metadata->nulls[metadata->adis_i] = true;
		metadata->adis_i += 1;
		return;
	}

	elog(DEBUG2, "Kafka-ADB: CSV field %lu: strlen=%lu, l=%lu, last_byte=%d", metadata->adis_i, strlen(value_str), value_l, (int) value_str[value_l - 1]);

	/* Check NULL */
	if (PointerIsValid(metadata->settings.null_string))
	{
		if (0 == strcmp(value_str, metadata->settings.null_string))
		{
			metadata->nulls[metadata->adis_i] = true;
			metadata->adis_i += 1;
			return;
		}
	}

	/* TODO: An error callback can be added here */
	metadata->nulls[metadata->adis_i] = false;
	metadata->datums[metadata->adis_i] = InputFunctionCall(
					  &metadata->adis[metadata->adis_i].io_fn_textual.iofunc,
														   value_str,
				   metadata->adis[metadata->adis_i].io_fn_textual.typioparam,
					  metadata->adis[metadata->adis_i].io_fn_textual.attypmod
		);
	metadata->adis_i += 1;
}

/**
 * A callback called by libcsv when a whole record has been parsed.
 */
static void
csv_record_callback(int finish_char, void *metadata_ptr)
{
	CSVDeserializationMetadata *metadata = (CSVDeserializationMetadata *) metadata_ptr;

	if (metadata->settings.ignore_one_tuple)
	{
		metadata->settings.ignore_one_tuple = false;
		return;
	}

	/* Process empty line */

	/*
	 * Known limitation: NULLs are ignored in tables with a single column
	 */
	if (metadata->adis_i == 1 && metadata->nulls[0])
	{
		metadata->adis_i = 0;
		return;
	}

	/* Fill missing fields with NULLs */
	for (; metadata->adis_i < (size_t) metadata->tupledesc->natts; metadata->adis_i++)
	{
		metadata->nulls[metadata->adis_i] = true;
	}

	metadata->result = lappend(metadata->result, heap_form_tuple(metadata->tupledesc, metadata->datums, metadata->nulls));

	/* Set to 0 for the next iteration */
	metadata->adis_i = 0;
}

List *
deserialize_csv(CSVDeserializationState state, void *data, size_t data_l)
{
	AssertArg(PointerIsValid(state));
	Assert(PointerIsValid(state->metadata));
	Assert(PointerIsValid(state->parser));

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_csv") == FaultInjectorTypeSkip)
	{
		data = state->metadata->data;
		data_l = state->metadata->data_l;
	}
#endif

	if (!PointerIsValid(data) || data_l < 1)
		return NIL;

	state->metadata->settings.ignore_one_tuple = state->metadata->settings.ignore_header;

	if (data_l != csv_parse(
							state->parser,
							data, data_l,
					 csv_field_callback, csv_record_callback, state->metadata
							))
	{
		int			err = csv_error(state->parser);

		elog(ERROR, "Kafka-ADB: Failed to deserialize CSV: %s [%d]", csv_strerror(err), err);
	}

	if (csv_fini(state->parser, csv_field_callback, csv_record_callback, state->metadata))
	{
		int			err = csv_error(state->parser);

		elog(ERROR, "Kafka-ADB: Failed to deserialize CSV: %s [%d]", csv_strerror(err), err);
	}

	List	   *result = state->metadata->result;

	state->metadata->result = NIL;
	return result;
}

void
finish_deserialization_csv(CSVDeserializationState state)
{
	AssertArg(PointerIsValid(state));
	Assert(PointerIsValid(state->parser));

	csv_free(state->parser);
	/* Other fields are freed by context destruction */
}
