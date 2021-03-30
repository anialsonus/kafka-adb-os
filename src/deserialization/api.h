#ifndef KADB_FDW_DESERIALIZATION_API_INCLUDED
#define KADB_FDW_DESERIALIZATION_API_INCLUDED

/*
 * Deserialization interface, incapsulating concrete deserialization methods.
 */

#include <postgres.h>
#include <access/tupdesc.h>


/* Opaque binary object used to store deserialization metadata */
typedef struct DeserializationMetadataObject *DeserializationMetadata;


/**
 * Prepare 'DeserializationMetadata' and initialize the deserialization
 * implementation requested in 'options'.
 *
 * This method calls 'elog(ERROR)' if an error is found in 'options'.
 */
DeserializationMetadata prepare_deserialization(TupleDesc tupledesc, List *options);

/**
 * Deserialize binary 'data' of length 'data_l'.
 *
 * This method calls 'elog(ERROR)' if a parse error happens.
 *
 * @return a list of HeapTuples, allocated by palloc in CurrentMemoryContext.
 * @return NIL (empty list) if 'data' is NULL or 'data_l' is 0
 */
List	   *deserialize(DeserializationMetadata metadata, void *data, size_t data_l);

/**
 * Finish the deserialization: close all opened structures, release memory, etc.
 */
void		finish_deserialization(DeserializationMetadata metadata);


#endif   /* // KADB_FDW_DESERIALIZATION_API_INCLUDED */
