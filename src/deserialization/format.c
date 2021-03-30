#include "format.h"


#define STRCASEEQ(a, b) (pg_strcasecmp(a, b) == 0)


enum DeserializationFormat
resolve_deserialization_format(const char *name)
{
	if (!PointerIsValid(name))
		return DESERIALIZATION_FORMAT_INVALID;

	if (STRCASEEQ(name, "avro"))
		return AVRO;
	if (STRCASEEQ(name, "csv"))
		return CSV;
	if (STRCASEEQ(name, "text"))
		return TEXT;

	return DESERIALIZATION_FORMAT_INVALID;
}
