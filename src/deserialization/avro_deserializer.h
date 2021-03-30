#ifndef KADB_FDW_DESERIALIZATION_AVRO_DESERIALIZER_INCLUDED
#define KADB_FDW_DESERIALIZATION_AVRO_DESERIALIZER_INCLUDED

/*
 * AVRO decoder implementation.
 */

#include <postgres.h>

#include <access/htup.h>
#include <access/tupdesc.h>


/* Opaque binary object used by deserializer to store a resolved AVRO schema. */
typedef struct AvroDeserializationMetadataObject *AvroDeserializationMetadata;


/**
 * Initialize libavro to work in PostgreSQL environment. Must be called on
 * every host where libavro is going to be used.
 */
void		initialize_libavro_for_postgres(void);

/**
 * Resolve the given 'json' as an AVRO schema.
 *
 * The result is allocated by palloc in CurrentMemoryContext.
 *
 * @param json may be NULL, if no schema is provided by user. In this case,
 * schema is taken from each incoming message independently.
 *
 * @note 'tupledesc' is not copied. It must be allocated in a
 * sufficiently-long-living memory context.
 */
AvroDeserializationMetadata prepare_deserialization_metadata_avro(TupleDesc tupledesc, const char *json);

/**
 * Deserialize binary 'data' of length 'data_l'.
 *
 * @return a list of HeapTuples, allocated by palloc in CurrentMemoryContext
 * @return NIL (empty list) if 'data' is NULL or 'data_l' is 0
 *
 * @note 'data' is expected to be in Object Container File format
 */
List	   *deserialize_avro(AvroDeserializationMetadata ds_metadata, void *data, size_t data_l);


#endif   /* //
								 * KADB_FDW_DESERIALIZATION_AVRO_DESERIALIZER_I
								 * NCLUDED */
