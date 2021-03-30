#include "text_deserializer.h"

#include <commands/defrem.h>

#include "settings.h"
#include "deserialization/attribute_postgres.h"


struct TextDeserializationState
{
	AttributeDeserializationInfo adi;
	TupleDesc	tupledesc;
#ifdef FAULT_INJECTOR
	/* Raw data to use for tests */
	char	   *data;
	size_t		data_l;
#endif
};

TextDeserializationState
prepare_deserialization_text(TupleDesc tupledesc, List *options)
{
	TextDeserializationState result = palloc(sizeof(struct TextDeserializationState));

	/* Ensure the table definition contains exactly one attribute */
	if (tupledesc->natts != 1)
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: 'text' format can only be applied to a table with a single attribute (column)")));

	fill_attribute_deserialization_info(&result->adi, tupledesc, 0);

	if (result->adi.is_dropped)
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Kafka-ADB: 'text' format can only be applied to a table with a single attribute (column)")));

	result->tupledesc = tupledesc;

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_text") == FaultInjectorTypeSkip)
	{
		char	   *inject_data = defGetString(get_option(options, KADB_SETTING_TEXT_DATA_ON_INJECT));

		result->data_l = strlen(inject_data) + 1;
		result->data = palloc(result->data_l);
		strcpy(result->data, inject_data);
	}
#endif

	return result;
}

List *
deserialize_text(TextDeserializationState state, void *data, size_t data_l)
{
	AssertArg(PointerIsValid(state));

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_text") == FaultInjectorTypeSkip)
	{
		data = state->data;
		data_l = state->data_l;
	}
#endif

	Datum		values[1];
	bool		nulls[1];

	if (!PointerIsValid(data) || data_l < 1 || strlen((char *) data) < 1)
	{
		nulls[0] = true;
	}
	else
	{
		nulls[0] = false;
		StringInfo	buffer = makeStringInfo();

		appendStringInfoString(buffer, (char *) data);
		values[0] = InputFunctionCall(
									  &state->adi.io_fn_textual.iofunc,
									  buffer->data,
									  state->adi.io_fn_textual.typioparam,
									  state->adi.io_fn_textual.attypmod
			);
		pfree(buffer->data);
		pfree(buffer);
	}

	return list_make1(heap_form_tuple(state->tupledesc, values, nulls));
}
