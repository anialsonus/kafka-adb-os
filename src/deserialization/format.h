#ifndef KADB_FDW_DESERIALIZATION_FORMAT_INCLUDED
#define KADB_FDW_DESERIALIZATION_FORMAT_INCLUDED

/*
 * Resolution of deserialization formats.
 */

#include <postgres.h>


/**
 * Deserialization formats supported by Kafka-ADB
 */
enum DeserializationFormat
{
	AVRO,
	CSV,
	TEXT,
	DESERIALIZATION_FORMAT_INVALID
}	DeserializationFormat;


/**
 * @return a value of 'DeserializationFormat', or
 * 'DESERIALIZATION_FORMAT_INVALID' if no format matches the given 'name'.
 */
enum DeserializationFormat resolve_deserialization_format(const char *name);


#endif   /* KADB_FDW_DESERIALIZATION_FORMAT_INCLUDED */
