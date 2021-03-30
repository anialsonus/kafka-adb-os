#include "api.h"

#include <nodes/parsenodes.h>
#include <utils/faultinjector.h>

#include "settings.h"
#include "deserialization/avro_deserializer.h"
#include "deserialization/csv_deserializer.h"
#include "deserialization/text_deserializer.h"
#include "deserialization/format.h"


typedef struct DeserializationMetadataObject
{
	enum DeserializationFormat format;
	void	   *data;
}	DeserializationMetadataObject;


DeserializationMetadata
prepare_deserialization(TupleDesc tupledesc, List *options)
{
	AssertArg(PointerIsValid(tupledesc));
	AssertArg(PointerIsValid(options));

	DeserializationMetadata result = palloc(sizeof(DeserializationMetadataObject));

	/* Deserialization format is validated when options are parsed */
	result->format = resolve_deserialization_format(defGetString(get_option(options, KADB_SETTING_FORMAT)));
	switch (result->format)
	{
		case AVRO:
			initialize_libavro_for_postgres();
			result->data = prepare_deserialization_metadata_avro(
																 tupledesc,
																 PointerIsValid(get_option(options, KADB_SETTING_AVRO_SCHEMA)) ? defGetString(get_option(options, KADB_SETTING_AVRO_SCHEMA)) : NULL
				);
			break;
		case CSV:
			result->data = prepare_deserialization_csv(tupledesc, options);
			break;
		case TEXT:
			result->data = prepare_deserialization_text(tupledesc, options);
			break;
		default:
			Assert(false);
			break;
	}

	return result;
}

List *
deserialize(DeserializationMetadata metadata, void *data, size_t data_l)
{
	AssertArg(PointerIsValid(metadata));
	AssertArg(metadata->format >= 0 && metadata->format < DESERIALIZATION_FORMAT_INVALID);

	switch (metadata->format)
	{
		case AVRO:
			return deserialize_avro((AvroDeserializationMetadata) metadata->data, data, data_l);
		case CSV:
			return deserialize_csv((CSVDeserializationState) metadata->data, data, data_l);
		case TEXT:
			return deserialize_text((TextDeserializationState) metadata->data, data, data_l);
		default:
			Assert(false);
			return NIL;
	}
}

void
finish_deserialization(DeserializationMetadata metadata)
{
	AssertArg(PointerIsValid(metadata));
	AssertArg(metadata->format >= 0 && metadata->format < DESERIALIZATION_FORMAT_INVALID);

	switch (metadata->format)
	{
		case AVRO:
			break;
		case CSV:
			finish_deserialization_csv((CSVDeserializationState) metadata->data);
			break;
		case TEXT:
			break;
		default:
			Assert(false);
			break;
	}

	pfree(metadata);
}
