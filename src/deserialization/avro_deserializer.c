#include "avro_deserializer.h"

#include <errno.h>
#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define le32toh(x) OSSwapLittleToHostInt32(x)
#else
#include <endian.h>
#endif
#include <float.h>
#include <inttypes.h>
#include <stdio.h>

#include <gmp.h>
#include <avro.h>

#include <access/htup_details.h>
#include <pgtime.h>
#include <utils/builtins.h>
#include <utils/faultinjector.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <utils/datetime.h>

#include "deserialization/attribute_postgres.h"


/* The size of a buffer to use for 'strftime' calls */
#define STRFTIME_BUFFER_SIZE 128

/* Several widely-known constants */
#define SECONDS_IN_HOUR (60 * 60)
#define SECONDS_IN_MINUTE (60)
#define MILLISECONDS_IN_SECOND (1000)
#define MILLISECONDS_IN_SECOND_LOG10 (3)
#define MICROSECONDS_IN_SECOND (1000000L)
#define MICROSECONDS_IN_SECOND_LOG10 (6)
#define MICROSECONDS_IN_MILLISECOND (1000)

/* A mask to extract the first bit of a byte */
#define FIRST_BIT_OF_BYTE ((unsigned char)0x80U)
/* A mask to extract a byte (e.g. from an integer) */
#define BYTE ((unsigned char)0xffU)


/**
 * AVRO "logical" types supported by Kafka-ADB.
 * This structure is an intermediate "mapping" between AVRO and PostgreSQL
 * types.
 */
typedef enum AvroLogicalType
{
	AVRO_LT_PRIMITIVE,			/* Not a logical type */
	AVRO_CT_BYTES,				/* 'bytes' and 'fixed' */
	AVRO_LT_DECIMAL,
	AVRO_LT_DATE,
	AVRO_LT_TIME,				/* Both time-millis and time-micros */
	AVRO_LT_TIMESTAMP_3,		/* timestamp-millis */
	AVRO_LT_TIMESTAMP_6,		/* timestamp-micros */
	AVRO_LT_DURATION
}	AvroLogicalType;

/**
 * 'AttributeDeserializationInfo' with extra fields for AVRO types' resolution.
 */
typedef struct AvroAttributeDeserializationInfo
{
	AttributeDeserializationInfo adi;
	AvroLogicalType expected_logical_type;
	avro_type_t expected_primitive_type;
}	AvroAttributeDeserializationInfo;

/* Definition is in the header */
struct AvroDeserializationMetadataObject
{
	TupleDesc	tupledesc;
	bool		is_schema_provided;
	avro_schema_t schema;
	avro_value_t value;
	AvroAttributeDeserializationInfo *adis;
};


/**
 * An implementation of 'avro_allocator_t' for PostgreSQL.
 *
 * palloc() in CurrentMemoryContext is used to allocate memory. This behaviour
 * is ASSUMED by this module: some memory, when it is expected to be allocated
 * in a short-living context, is not freed.
 */
static void *
avro_postgres_allocator(void *user_data, void *ptr, size_t osize, size_t nsize)
{
	/* free */
	if (nsize == 0)
	{
		if (PointerIsValid(ptr))
			pfree(ptr);
		return NULL;
	}
	/* malloc */
	if (osize == 0)
	{
		return palloc(nsize);
	}
	/* realloc */
	void	   *result = palloc(nsize);

	memcpy(result, ptr, osize);
	pfree(ptr);
	return result;
}

/**
 * An implementation of 'alloc' function used by GNU MP for PostgreSQL.
 */
static void *
gmp_postgres_alloc(size_t size)
{
	return palloc(size);
}

/**
 * An implementation of 'realloc' function used by GNU MP for PostgreSQL.
 */
static void *
gmp_postgres_realloc(void *ptr, size_t old_size, size_t new_size)
{
	return repalloc(ptr, new_size);
}

/**
 * An implementation of 'free' function used by GNU MP for PostgreSQL.
 */
static void
gmp_postgres_free(void *ptr, size_t size)
{
	pfree(ptr);
}

void
initialize_libavro_for_postgres(void)
{
	avro_set_allocator(avro_postgres_allocator, NULL);
	mp_set_memory_functions(gmp_postgres_alloc, gmp_postgres_realloc, gmp_postgres_free);
}

/**
 * Convert a Postgres type to an AVRO type.
 *
 * If an unknown type is found, an error is reported by elog.
 *
 * @param att is used to extract Postgres type information
 * @param adi is filled with expected AVRO type data
 */
static void
convert_postgres_type_to_avro_type(Form_pg_attribute att, AvroAttributeDeserializationInfo * adi)
{
	Oid			typoid = att->atttypid;

	adi->expected_logical_type = AVRO_LT_PRIMITIVE;
	adi->expected_primitive_type = AVRO_STRING;

	switch (typoid)
	{
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
			break;
		case INT4OID:
			adi->expected_primitive_type = AVRO_INT32;
			break;
		case INT8OID:
			adi->expected_primitive_type = AVRO_INT64;
			break;
		case FLOAT4OID:
			adi->expected_primitive_type = AVRO_FLOAT;
			break;
		case FLOAT8OID:
			adi->expected_primitive_type = AVRO_DOUBLE;
			break;
		case BOOLOID:
			adi->expected_primitive_type = AVRO_BOOLEAN;
			break;
		case BYTEAOID:
			adi->expected_logical_type = AVRO_CT_BYTES;
			break;
		case NUMERICOID:
			adi->expected_logical_type = AVRO_LT_DECIMAL;
			break;
		case DATEOID:
			adi->expected_logical_type = AVRO_LT_DATE;
			adi->expected_primitive_type = AVRO_INT32;
			break;
		case TIMEOID:
			adi->expected_logical_type = AVRO_LT_TIME;
			/* Primitive type is determined at parse time in this case */
			break;
		case TIMESTAMPOID:
			{
				int32		typmod = att->atttypmod;

				if (typmod == -1)		/* TIMESTAMP */
					adi->expected_logical_type = AVRO_LT_TIMESTAMP_6;
				else if (typmod <= 3)	/* TIMESTAMP(N) */
					adi->expected_logical_type = AVRO_LT_TIMESTAMP_3;
				else	/* TIMESTAMP(N) */
					adi->expected_logical_type = AVRO_LT_TIMESTAMP_6;
			}
			adi->expected_primitive_type = AVRO_INT64;
			break;
		case INTERVALOID:
			adi->expected_logical_type = AVRO_LT_DURATION;
			adi->expected_primitive_type = AVRO_FIXED;
			break;
		default:
			break;				/* AVRO_STRING for unknown types */
	}
}

/**
 * Fill 'metadata->value' using 'metadata->schema'.
 *
 * @note This method ASSUMES 'metadata->schema' is valid
 */
static void
schema_to_value(AvroDeserializationMetadata metadata)
{
	Assert(PointerIsValid(metadata));

	int			err;

	if ((err = avro_generic_value_new(avro_generic_class_from_schema(metadata->schema), &metadata->value)))
		elog(ERROR, "Kafka-ADB: Failed to resolve AVRO schema: %s [%d]", strerror(err), err);
}

AvroDeserializationMetadata
prepare_deserialization_metadata_avro(TupleDesc tupledesc, const char *json)
{
	Assert(PointerIsValid(tupledesc));

	int			err;

	AvroDeserializationMetadata result = (AvroDeserializationMetadata) palloc(sizeof(struct AvroDeserializationMetadataObject));

	result->tupledesc = tupledesc;

	result->is_schema_provided = false;
	if (PointerIsValid(json))
	{
		result->is_schema_provided = true;
		if ((err = avro_schema_from_json(json, 0, &result->schema, NULL)))
			elog(ERROR, "Kafka-ADB: Failed to parse AVRO schema: %s [%d]", strerror(err), err);
		schema_to_value(result);
	}

	result->adis = (AvroAttributeDeserializationInfo *) palloc(sizeof(AvroAttributeDeserializationInfo) * tupledesc->natts);
	for (int i = 0; i < tupledesc->natts; i++)
	{
		fill_attribute_deserialization_info(&result->adis[i].adi, tupledesc, i);
		if (result->adis[i].adi.is_dropped)
			continue;
		convert_postgres_type_to_avro_type(tupledesc->attrs[i], &result->adis[i]);
	}

	return result;
}

/**
 * Convert an AVRO 'value' of primitive type to a string representation, written
 * in 'buff'.
 */
static void
translate_avro_value_of_primitive_type(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	int			err = EINVAL;

	switch (adi->expected_primitive_type)
	{
		case AVRO_STRING:
			{
				const char *result;
				size_t		result_l;

				err = avro_value_get_string(&value, &result, &result_l);
				if (!err)
					appendStringInfoString(buff, result);
			}
			break;
		case AVRO_INT32:
			{
				int32_t		result;

				err = avro_value_get_int(&value, &result);
				if (!err)
					appendStringInfo(buff, "%" PRId32, result);
			}
			break;
		case AVRO_INT64:
			{
				int64_t		result;

				err = avro_value_get_long(&value, &result);
				if (!err)
					appendStringInfo(buff, "%" PRId64, result);
			}
			break;
		case AVRO_FLOAT:
			{
				float		result;

				err = avro_value_get_float(&value, &result);
				if (!err)
					appendStringInfo(buff, "%.*g", FLT_DIG + 3, result);
			}
			break;
		case AVRO_DOUBLE:
			{
				double		result;

				err = avro_value_get_double(&value, &result);
				if (!err)
					appendStringInfo(buff, "%.*g", DBL_DIG + 3, result);
			}
			break;
		case AVRO_BOOLEAN:
			{
				int			result;

				err = avro_value_get_boolean(&value, &result);
				if (!err)
					appendStringInfoString(buff, (bool) result ? "TRUE" : "FALSE");
			}
			break;
		default:
			Assert(false);
			elog(ERROR, "Kafka-ADB: Failed assertion: Unexpected %d", adi->expected_primitive_type);
	}

	if (err)
		elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);
}

/**
 * Convert an AVRO 'value' of complex type 'fixed' or primitive type 'bytes' to
 * a string representation, written in 'buff'.
 */
static void
translate_avro_value_bytes(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	Assert(adi->expected_logical_type == AVRO_CT_BYTES);

	const unsigned char *result;
	size_t		result_l;

	avro_type_t actual_type = avro_value_get_type(&value);

	int			err;

	switch (actual_type)
	{
		case AVRO_BYTES:
			err = avro_value_get_bytes(&value, (const void **) &result, &result_l);
			break;
		case AVRO_FIXED:
			err = avro_value_get_fixed(&value, (const void **) &result, &result_l);
			break;
		default:
			Assert(false);
			elog(ERROR, "Kafka-ADB: Failed assertion: Unexpected %d", actual_type);
	}
	if (err)
		elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);

	if (result_l == 0)
	{
		elog(DEBUG1, "Kafka-ADB: BYTEA of zero length received in AVRO attribute %d", avro_attid);
		return;
	}

	/* PostgreSQL 'BYTEA' format */
	appendStringInfoString(buff, "\\x");
	for (size_t i = 0; i < result_l; i++)
	{
		appendStringInfo(buff, "%02x", result[i] & BYTE);
	}
}

/**
 * Print the date represented by 'days' since the Epoch into 'buff', in the
 * ISO 8601 date format (%Y-%m-%d).
 */
static void
epoch_days_to_date_string(int32_t days, StringInfo buff)
{
	int			year;
	int			month;
	int			day;

	j2date(days + UNIX_EPOCH_JDATE, &year, &month, &day);

	appendStringInfo(buff, "%04d-%02d-%02d", year, month, day);
}

/**
 * Convert an AVRO 'value' of logical type 'date' to a string representation,
 * written in 'buff'.
 *
 * == AVRO specification ==
 *
 * The date logical type represents a date within the calendar,
 * with no reference to a particular time zone or time of day.
 *
 * A date logical type annotates an Avro int, where the int stores the number of
 * days from the unix epoch, 1 January 1970 (ISO calendar).
 */
static void
translate_avro_value_of_lt_date(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	Assert(adi->expected_logical_type == AVRO_LT_DATE);

	int32_t		result;

	int			err = avro_value_get_int(&value, &result);

	if (err)
		elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);

	epoch_days_to_date_string(result, buff);
}

/**
 * Print the time represented by 'fracseconds' since midnight,
 * in PostgreSQL 'TIME' format.
 *
 * @param fracseconds time of day, expressed in fractions of second. A concrete
 * fraction is specified by 'frac_multiplier'
 * @param frac_multiplier specifier of the fraction. A value, such that
 * 'fracseconds / frac_multipler = seconds'
 * @param frac_width specifier of the fraction: log10(frac_multiplier); the
 * number of leading zeros.
 */
static void
fracseconds_to_time_string(int64_t fracseconds, int64_t frac_multiplier, int frac_width, StringInfo buff)
{
	int32_t		hours = fracseconds / (frac_multiplier * SECONDS_IN_HOUR);

	fracseconds = fracseconds % (frac_multiplier * SECONDS_IN_HOUR);
	int32_t		minutes = fracseconds / (frac_multiplier * SECONDS_IN_MINUTE);

	fracseconds = fracseconds % (frac_multiplier * SECONDS_IN_MINUTE);
	int32_t		seconds = fracseconds / frac_multiplier;
	int64_t		fracs = fracseconds % frac_multiplier;

	appendStringInfo(buff, "%02d:%02d:%02d.%0*ld", hours, minutes, seconds, frac_width, fracs);
}

/**
 * Convert an AVRO 'value' of logical types 'time-millis' or 'time-micros'
 * to a string representation, written in 'buff'.
 *
 * == AVRO specification (millis) ==
 *
 * The time-millis logical type represents a time of day, with no reference to
 * a particular calendar, time zone or date, with a precision of one
 * millisecond.
 *
 * A time-millis logical type annotates an Avro int, where the int stores the
 * number of milliseconds after midnight, 00:00:00.000.
 *
 * == AVRO specification (micros) ==
 *
 * The time-micros logical type represents a time of day, with no reference to
 * a particular calendar, time zone or date, with a precision of one
 * microsecond.
 *
 * A time-micros logical type annotates an Avro long, where the long stores the
 * number of microseconds after midnight, 00:00:00.000000.
 */
static void
translate_avro_value_of_lt_time(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	Assert(adi->expected_logical_type == AVRO_LT_TIME);

	avro_type_t actual_type = avro_value_get_type(&value);

	switch (actual_type)
	{
		case AVRO_INT32:
			{
				int			err;
				int32_t		result;

				err = avro_value_get_int(&value, &result);
				if (err)
					elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);
				fracseconds_to_time_string(result, MILLISECONDS_IN_SECOND, MILLISECONDS_IN_SECOND_LOG10, buff);
			}
			break;
		case AVRO_INT64:
			{
				int			err;
				int64_t		result;

				err = avro_value_get_long(&value, &result);
				if (err)
					elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);
				fracseconds_to_time_string(result, MICROSECONDS_IN_SECOND, MICROSECONDS_IN_SECOND_LOG10, buff);
			}
			break;
		default:
			Assert(false);
			elog(ERROR, "Kafka-ADB: Failed assertion: Unexpected %d", actual_type);
	}
}

/**
 * Print the timestamp represented by 'useconds' (microseconds) since the Epoch
 * into 'buff', in the ISO 8601 date format combined with PostgreSQL TIME
 * format (%Y-%m-%d %H:%M:%S.<microseconds> ).
 */
static void
epoch_useconds_to_timestamp_string(int64_t useconds, StringInfo buff)
{
	struct pg_tm tm;			/* Timestamp split into parts */
	fsec_t		tm_fsec;		/* Fraction of a second */

	memset(&tm, 0, sizeof(struct pg_tm));

	/*
	 * timestamp2tm() is a PostgreSQL function. To represent time, PostgreSQL
	 * uses its own epoch which starts at 2000-01-01 00:00:00. In order to
	 * convert the result to UNIX epoch, subtract 30 years.
	 *
	 * This is NOT correct. There are leap seconds between UNIX and PostgreSQL
	 * epoch dates; thus timestamp2tm() should produce results which differ
	 * from mktime() ones (which counts dates from UNIX epoch) not just by 30
	 * years, but by 30 years and 22 seconds (there were 22 leap seconds
	 * between years 1970 and 2000).
	 *
	 * However, the results produced by both methods ACTUALLY differ only by
	 * 30 years (and zero seconds); thus we rely on the match of
	 * implementation results of mktime() and timestamp2tm().
	 *
	 * The reason why PostgreSQL implementation is incorrect is that
	 * timestamp2tm() uses TMODULO() macros internally, which assumes the
	 * length of a day (in seconds) does not depend on the date. This is an
	 * invalid assumption. An interesting question is why Linux / glibc
	 * implementation of mktime() does not account for leap seconds...
	 */

	useconds -= USECS_PER_DAY * (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);

	if (timestamp2tm(useconds, NULL, &tm, &tm_fsec, NULL, NULL) < 0)
	{
		elog(ERROR, "Kafka-ADB: Failed to convert AVRO value '%" PRId64 "' to a PostgreSQL timestamp", useconds);
	}

	appendStringInfo(buff, "%04d-%02d-%02d %02d:%02d:%02d.%06d", tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, (int) tm_fsec);
}

/**
 * Convert an AVRO 'value' of logical types 'timestamp-millis' and
 * 'timestamp-micros' to a string representation, written in 'buff'.
 *
 * == AVRO specification (millis) ==
 *
 * The timestamp-millis logical type represents an instant on the global
 * timeline, independent of a particular time zone or calendar, with
 * a precision of one millisecond.
 *
 * A timestamp-millis logical type annotates an Avro long, where the long stores
 * the number of milliseconds from the unix epoch,
 * 1 January 1970 00:00:00.000 UTC.
 *
 * == AVRO specification (micros) ==
 *
 * The timestamp-micros logical type represents an instant on the global
 * timeline, independent of a particular time zone or calendar, with
 * a precision of one microsecond.
 *
 * A timestamp-micros logical type annotates an Avro long, where the long stores
 * the number of microseconds from the unix epoch,
 * 1 January 1970 00:00:00.000000 UTC.
 */
static void
translate_avro_value_of_lt_timestamp(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	Assert(adi->expected_logical_type == AVRO_LT_TIMESTAMP_3 || adi->expected_logical_type == AVRO_LT_TIMESTAMP_6);

	int64_t		result;
	int			err = avro_value_get_long(&value, &result);

	if (err)
		elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);

	switch (adi->expected_logical_type)
	{
		case AVRO_LT_TIMESTAMP_3:
			result *= MICROSECONDS_IN_MILLISECOND;
			break;
		case AVRO_LT_TIMESTAMP_6:
			break;
		default:
			Assert(false);
			elog(ERROR, "Kafka-ADB: Failed assertion: Unexpected %d", adi->expected_logical_type);
	}

	epoch_useconds_to_timestamp_string(result, buff);
}

/**
 * Convert an AVRO 'value' of logical type 'duration' to a string
 * representation, written in 'buff'.
 *
 * == AVRO specification ==
 *
 * The duration logical type represents an amount of time defined by a number
 * of months, days and milliseconds. This is not equivalent to a number
 * of milliseconds, because, depending on the moment in time from which
 * the duration is measured, the number of days in the month and number of
 * milliseconds in a day may differ. Other standard periods such as years,
 * quarters, hours and minutes can be expressed through these basic periods.
 *
 * A duration logical type annotates Avro fixed type of size 12, which stores
 * three little-endian unsigned integers that represent durations at different
 * granularities of time. The first stores a number in months, the second stores
 * a number in days, and the third stores a number in milliseconds.
 */
static void
translate_avro_value_of_lt_duration(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	Assert(adi->expected_logical_type == AVRO_LT_DURATION);

	const char *result;
	size_t		result_l;

	int			err = avro_value_get_fixed(&value, (const void **) &result, &result_l);

	if (err)
		elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);
	if (result_l != 12)
		elog(ERROR, "Kafka-ADB: Failed to convert AVRO 'duration': Provided 'fixed' of length %lu (expected 12)", result_l);

	/* 'endian.h' enables to make no assumptions about endianness on the host */
	uint32_t	months = le32toh(*(uint32_t *) result);
	uint32_t	days = le32toh(*(uint32_t *) (result + sizeof(uint32_t)));
	uint32_t	milliseconds = le32toh(*(uint32_t *) (result + 2 * sizeof(uint32_t)));

	appendStringInfo(buff, "@ %u month %u day %u millisecond", months, days, milliseconds);
}

/**
 * Print the absolute value of a decimal number represented by its
 * 'twos_complement' of 'length' bytes into 'buff'. 'twos_complement' MUST be
 * in big-endian order.
 *
 * See https://www.cs.cornell.edu/~tomf/notes/cps104/twoscomp.html for details
 * on what two's complement is and how to work with it.
 */
static void
twos_complement_to_human_readable_decimal_abs(const void *twos_complement, size_t length, StringInfo buff)
{
	const unsigned char *digits = (const unsigned char *) twos_complement;
	bool		is_negative = (digits[0] & FIRST_BIT_OF_BYTE);

	StringInfo	digits_printed = makeStringInfo();

	size_t		i;

	for (i = 0; i < length; i++)
	{
		/*
		 * Print the result in hex notation (this is easy). The result is then
		 * consumed by GNU MP.
		 */

		/*
		 * For negative numbers, print negated bytes (two's complement
		 * conversion, step 1).
		 */
		appendStringInfo(
						 digits_printed, "%02x",
						 (is_negative ? ~digits[i] : digits[i]) & BYTE
			);
	}

	mpz_t		number;

	/* Set GNU MP integer */
	if (mpz_init_set_str(number, digits_printed->data, 16))
	{
		mpz_clear(number);
		elog(ERROR, "Kafka-ADB: Failed to convert a decimal from AVRO (mpz_init_set_str). Original value '%s'", digits_printed->data);
	}

	/* For negative numbers, add 1 (two's complement conversion, step 2) */
	if (is_negative)
		mpz_add_ui(number, number, 1L);

	/* Print the result, now in decimal notation */
	const size_t snprintf_len = (digits_printed->len * 2) + 3;
	char	   *result = palloc(snprintf_len);
	const int	snprintf_result = gmp_snprintf(result, snprintf_len, "%Zd", number);

	if ((size_t) snprintf_result > snprintf_len)
	{
		mpz_clear(number);
		elog(ERROR, "Kafka-ADB: Failed to convert a decimal from AVRO (gmp_snprintf). Original value '%s'; gmp_snprintf: length %lu, return value %d", digits_printed->data, snprintf_len, snprintf_result);
	}
	mpz_clear(number);
	pfree(digits_printed->data);
	pfree(digits_printed);

	appendStringInfoString(buff, result);
	pfree(result);
}

/**
 * Convert an AVRO 'value' of logical type 'decimal' to a string
 * representation, written in 'buff'.
 *
 * == AVRO specification ==
 *
 * The decimal logical type represents an arbitrary-precision signed decimal
 * number of the form unscaled × 10-scale.
 *
 * A decimal logical type annotates Avro bytes or fixed types. The byte array
 * must contain the two's-complement representation of the unscaled integer
 * value in big-endian byte order.
 *
 * @note It is not possible to extract 'scale' using libavro-c. Thus we obtain
 * scale from the FOREIGN TABLE definition.
 */
static void
translate_avro_value_of_lt_decimal(avro_value_t value, AvroAttributeDeserializationInfo * adi, StringInfo buff, int avro_attid)
{
	Assert(adi->expected_logical_type == AVRO_LT_DECIMAL);

	const void *twos_complement;
	size_t		result_l;

	avro_type_t actual_type = avro_value_get_type(&value);

	int			err;

	switch (actual_type)
	{
		case AVRO_FIXED:
			err = avro_value_get_fixed(&value, &twos_complement, &result_l);
			break;
		case AVRO_BYTES:
			err = avro_value_get_bytes(&value, &twos_complement, &result_l);
			break;
		default:
			Assert(false);
			elog(ERROR, "Kafka-ADB: Failed assertion: Unexpected %d", actual_type);
	}
	if (err)
		elog(ERROR, "Kafka-ADB: Failed to retrieve AVRO attribute %d: %s [%d]", avro_attid, strerror(err), err);

	if (result_l == 0)
	{
		elog(WARNING, "Kafka-ADB: DECIMAL of zero length received in AVRO attribute %d; converted to '0'", avro_attid);
		appendStringInfoChar(buff, '0');
		return;
	}

	StringInfo	result = makeStringInfo();

	twos_complement_to_human_readable_decimal_abs(twos_complement, result_l, result);

	/* Determine sign using two's complement's properties */
	if (((unsigned char *) twos_complement)[0] & FIRST_BIT_OF_BYTE)
	{
		appendStringInfoChar(buff, '-');
	}

	/* Determine PostgreSQL NUMERIC scale */
	int32		typmod = adi->adi.io_fn_textual.attypmod;
	int32		scale = 0;

	if (typmod >= VARHDRSZ)
	{
		typmod -= VARHDRSZ;
		scale = ((uint32) typmod) & 0xffffU;
	}

	/* Print digits */
	int32		result_i = 0;

	for (; result_i < result->len - scale; ++result_i)
	{
		appendStringInfoChar(buff, result->data[result_i]);
	}
	if (result_i == 0)
	{
		/* No digits were printed before radix. Add a leading zero */
		appendStringInfoChar(buff, '0');
	}

	appendStringInfoChar(buff, '.');

	while (result->len < scale)
	{
		/* Leading zeros after radix */
		appendStringInfoChar(buff, '0');
		scale -= 1;
	}
	for (; result_i < result->len; result_i++)
	{
		appendStringInfoChar(buff, result->data[result_i]);
	}

	pfree(result->data);
	pfree(result);
}

/**
 * Convert an AVRO 'value' to a Postgres Datum object.
 */
static void
translate_avro_value_to_postgres_datum(AvroAttributeDeserializationInfo * adi, avro_value_t * value, Datum *result, bool *is_null, int avro_attid)
{
	Assert(PointerIsValid(adi));
	Assert(PointerIsValid(value));
	Assert(PointerIsValid(result));
	Assert(PointerIsValid(is_null));

	/* Process NULL values */
	*is_null = false;
	avro_value_t actual_value;
	avro_type_t actual_type = avro_value_get_type(value);

	if (actual_type == AVRO_NULL)
	{
		*is_null = true;
		return;
	}
	else if (actual_type == AVRO_UNION)
	{
		avro_value_get_current_branch(value, &actual_value);
		actual_type = avro_value_get_type(&actual_value);
		if (actual_type == AVRO_NULL)
		{
			*is_null = true;
			return;
		}
	}
	else
	{
		actual_value = *value;
	}

	/* Check the parsed schema matches the data, when possible */
	if (adi->expected_logical_type == AVRO_LT_PRIMITIVE)
	{
		if (adi->expected_primitive_type != actual_type)
		{
			elog(ERROR, "Kafka-ADB: AVRO primitive type of attribute %d (#%d) does not match the expected one (#%d)", avro_attid, actual_type, adi->expected_primitive_type);
		}
	}

	/* The converted value is then passed to InputFunctionCall */
	StringInfoData buff;

	initStringInfo(&buff);
	switch (adi->expected_logical_type)
	{
		case AVRO_LT_PRIMITIVE:
			translate_avro_value_of_primitive_type(actual_value, adi, &buff, avro_attid);
			break;
		case AVRO_CT_BYTES:
			translate_avro_value_bytes(actual_value, adi, &buff, avro_attid);
			break;
		case AVRO_LT_DECIMAL:
			translate_avro_value_of_lt_decimal(actual_value, adi, &buff, avro_attid);
			break;
		case AVRO_LT_DATE:
			translate_avro_value_of_lt_date(actual_value, adi, &buff, avro_attid);
			break;
		case AVRO_LT_TIME:
			translate_avro_value_of_lt_time(actual_value, adi, &buff, avro_attid);
			break;
		case AVRO_LT_TIMESTAMP_3:
		case AVRO_LT_TIMESTAMP_6:
			translate_avro_value_of_lt_timestamp(actual_value, adi, &buff, avro_attid);
			break;
		case AVRO_LT_DURATION:
			translate_avro_value_of_lt_duration(actual_value, adi, &buff, avro_attid);
			break;
		default:
			Assert(false);
			elog(ERROR, "Kafka-ADB: Failed assertion: Unexpected %d", adi->expected_logical_type);
	}

	PG_TRY();
	{
		*result = InputFunctionCall(
									&adi->adi.io_fn_textual.iofunc,
									buff.data,
									adi->adi.io_fn_textual.typioparam,
									adi->adi.io_fn_textual.attypmod
			);
	}
	PG_CATCH();
	{
		elog(WARNING, "Kafka-ADB: Conversion to PostgreSQL type failed for AVRO attribute %d '%s'", avro_attid, buff.data);
		PG_RE_THROW();
	}
	PG_END_TRY();
	pfree(buff.data);
}

#ifdef FAULT_INJECTOR
/**
 * A method to replace the actual deserialization in tests
 */
static List *
deserialize_dummy(AvroDeserializationMetadata ds_metadata)
{
	Datum	   *values = (Datum *) palloc(sizeof(Datum) * ds_metadata->tupledesc->natts);
	bool	   *nulls = (bool *) palloc0(sizeof(bool) * ds_metadata->tupledesc->natts);

	for (int i = 0; i < ds_metadata->tupledesc->natts; i++)
	{
		switch (ds_metadata->tupledesc->attrs[i]->atttypid)
		{
			case INT4OID:
				values[i] = Int32GetDatum(42);
				nulls[i] = false;
				break;
			case INT8OID:
				values[i] = Int64GetDatum(42);
				nulls[i] = false;
				break;
			default:
				nulls[i] = true;
		}
	}

	return list_make1(heap_form_tuple(ds_metadata->tupledesc, values, nulls));
}
#endif

List *
deserialize_avro(AvroDeserializationMetadata ds_metadata, void *data, size_t data_l)
{
	Assert(PointerIsValid(ds_metadata));
	Assert(PointerIsValid(ds_metadata->tupledesc));

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("kadb_fdw_inject_tuples") == FaultInjectorTypeSkip)
		return deserialize_dummy(ds_metadata);
#endif

	if (!PointerIsValid(data) || data_l < 1)
		return NIL;

	int			err;

	/* Apply libavro to the received buffer */
	FILE	   *data_fp = fmemopen(data, data_l, "r");
	avro_file_reader_t reader;

	if ((err = avro_file_reader_fp(data_fp, "Postgres", false, &reader)))
		elog(ERROR, "Kafka-ADB: Failed to read received AVRO bytes: %s [%d]", strerror(err), err);

	/* Prepare schema, if necessary */
	if (!ds_metadata->is_schema_provided)
	{
		/*
		 * These variables are allocated by palloc in the current context.
		 * They are freed before the next 'deserialize_avro()' call is made.
		 */
		ds_metadata->schema = avro_file_reader_get_writer_schema(reader);
		schema_to_value(ds_metadata);
	}
	avro_value_t *tuple_value = &ds_metadata->value;

	/* Iterate over records received in AVRO OCF format */
	List	   *result = NIL;
	Datum	   *values = (Datum *) palloc(sizeof(Datum) * ds_metadata->tupledesc->natts);
	bool	   *nulls = (bool *) palloc(sizeof(bool) * ds_metadata->tupledesc->natts);

	while (true)
	{
		err = avro_file_reader_read_value(reader, tuple_value);
		if (err == EOF)
			break;
		else if (err)
			elog(ERROR, "Kafka-ADB: Failed to deserialize AVRO: %s [%d]", avro_strerror(), err);

		/* Translate attributes: AVRO -> C -> Postgres */
		int			avro_i = 0;

		for (int i = 0; i < ds_metadata->tupledesc->natts; i++)
		{
			AvroAttributeDeserializationInfo *adi = &ds_metadata->adis[i];

			if (adi->adi.is_dropped)
			{
				nulls[i] = true;
				continue;
			}

			avro_value_t attribute_value;

			if ((err = avro_value_get_by_index(tuple_value, avro_i, &attribute_value, NULL)))
				elog(ERROR, "Kafka-ADB: Failed to read AVRO value: %s [%d]", strerror(err), err);
			translate_avro_value_to_postgres_datum(adi, &attribute_value, &values[i], &nulls[i], avro_i);
			avro_i += 1;
		}

		/* Form the tuple */
		result = lappend(result, heap_form_tuple(ds_metadata->tupledesc, values, nulls));
	}

	pfree(values);
	pfree(nulls);

	avro_value_reset(tuple_value);
	avro_file_reader_close(reader);
	fclose(data_fp);

	return result;
}
