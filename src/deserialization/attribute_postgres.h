#ifndef KADB_FDW_DESERIALIZATION_ATTRIBUTE_POSTGRES_INCLUDED
#define KADB_FDW_DESERIALIZATION_ATTRIBUTE_POSTGRES_INCLUDED

/*
 * Utility to assist in conversion of various datatypes to Postgres 'Datum's.
 */

#include <postgres.h>

#include <access/htup_details.h>
#include <utils/builtins.h>
#include <utils/faultinjector.h>
#include <utils/lsyscache.h>


/**
 * A complete set of data to call an input function.
 */
typedef struct FunctionCallCompleteData
{
	FmgrInfo	iofunc;
	Oid			typioparam;
	int32		attypmod;
}	FunctionCallCompleteData;

/**
 * A structure to hold a description of a single tuple attribute.
 *
 * The fields in this structure are extracted from 'TupleDesc'; its presence
 * solves two problems:
 * 1. 'iofunc' and 'typioparam' are obtained by a lengthy function call
 * 2. Dropped columns can be handled properly
 */
typedef struct AttributeDeserializationInfo
{
	/* Dropped attribute, does not require decoding */
	bool		is_dropped;
	/* Textual input function */
	FunctionCallCompleteData io_fn_textual;
}	AttributeDeserializationInfo;


/**
 * Fill the given 'adi' with appropriate data (from 'tupledesc' and extracted
 * from the database).
 */
void		fill_attribute_deserialization_info(AttributeDeserializationInfo * adi, TupleDesc tupledesc, size_t i);


#endif   /* //
								 * KADB_FDW_DESERIALIZATION_ATTRIBUTE_POSTGRES_
								 * INCLUDED */
