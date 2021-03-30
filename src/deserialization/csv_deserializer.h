#ifndef KADB_FDW_DESERIALIZATION_CSV_DESERIALIZER_INCLUDED
#define KADB_FDW_DESERIALIZATION_CSV_DESERIALIZER_INCLUDED

/*
 * CSV deserialization implementation.
 */

#include <postgres.h>

#include <access/tupdesc.h>


/* An opaque struct to store CSV deserialization runtime state */
typedef struct CSVDeserializationStateObject *CSVDeserializationState;


/**
 * Prepare to deserialize CSV data.
 */
CSVDeserializationState prepare_deserialization_csv(TupleDesc tupledesc, List *options);

/**
 * Deserialize binary 'data' of length 'data_l' from CSV format.
 *
 * @return a list of HeapTuples, allocated by palloc in CurrentMemoryContext,
 * or NIL if 'data' is empty
 */
List	   *deserialize_csv(CSVDeserializationState state, void *data, size_t data_l);

/**
 * Finish CSV deserialization and free the memory allocated to assist the
 * process.
 */
void		finish_deserialization_csv(CSVDeserializationState state);


#endif   /* //
								 * KADB_FDW_DESERIALIZATION_CSV_DESERIALIZER_IN
								 * CLUDED */
