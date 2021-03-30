#ifndef KADB_FDW_DESERIALIZATION_TEXT_DESERIALIZER_INCLUDED
#define KADB_FDW_DESERIALIZATION_TEXT_DESERIALIZER_INCLUDED

/*
 * TEXT deserialization implementation.
 *
 * 'text' format is a format when every Kafka message represents a single
 * attribute of a single tuple. As a result, a FOREIGN TABLE with such format
 * can have only one attribute. Each Kafka message is treated as text and passed
 * to the InputFunction of this single attribute.
 *
 * Empty messages are treated as NULLs. This makes it possible for the user to
 * count them.
 */

#include <postgres.h>

#include <access/tupdesc.h>


typedef struct TextDeserializationState *TextDeserializationState;

/**
 * Prepare parser to deserialize data in 'text' format.
 */
TextDeserializationState prepare_deserialization_text(TupleDesc tupledesc, List *options);

/**
 * Deserialize 'data' of length 'data_l' stored in format TEXT and return the
 * resulting list of tuples.
 *
 * The resulting list always contains exactly one tuple.
 *
 * If 'data' is NULL or an empty string, the only tuple in the result is a
 * NULL tuple.
 */
List	   *deserialize_text(TextDeserializationState state, void *data, size_t data_l);


#endif   /* KADB_FDW_DESERIALIZATION_TEXT_DESERIALIZER_I
								 * NCLUDED */
