/*
 * Kafka-ADB FOREIGN DATA WRAPPER implementation.
 */

#include <postgres.h>

#include <access/reloptions.h>
#include <cdb/cdbvars.h>
#include <foreign/fdwapi.h>
#include <nodes/nodes.h>

#include "execution.h"
#include "planning.h"
#include "settings.h"

#include "functions/auxiliary.h"
#include "functions/extra.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


PG_FUNCTION_INFO_V1(kadb_fdw_handler);
/**
 * Foreign data wrapper definition function.
 */
Datum
kadb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *result = makeNode(FdwRoutine);

	result->GetForeignRelSize = kadbGetForeignRelSize;
	result->GetForeignPaths = kadbGetForeignPaths;
	result->GetForeignPlan = kadbGetForeignPlan;
	result->BeginForeignScan = kadbBeginForeignScan;
	result->IterateForeignScan = kadbIterateForeignScan;
	result->ReScanForeignScan = kadbReScanForeignScan;
	result->EndForeignScan = kadbEndForeignScan;

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(kadb_fdw_validator);
/**
 * Foreign data wrapper validator method.
 */
Datum
kadb_fdw_validator(PG_FUNCTION_ARGS)
{
	/* We only run validator on dispatcher */
	if (Gp_role != GP_ROLE_DISPATCH)
		PG_RETURN_VOID();

	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));

	validate_options(&options, false);
	PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(kadb_commit_offsets);
PG_FUNCTION_INFO_V1(kadb_load_offsets_at_timestamp);
PG_FUNCTION_INFO_V1(kadb_load_offsets_earliest);
PG_FUNCTION_INFO_V1(kadb_load_offsets_latest);
PG_FUNCTION_INFO_V1(kadb_load_offsets_committed);
PG_FUNCTION_INFO_V1(kadb_load_partitions);

PG_FUNCTION_INFO_V1(kadb_partitions_obtain);
PG_FUNCTION_INFO_V1(kadb_partitions_clean);
PG_FUNCTION_INFO_V1(kadb_partitions_reset);
PG_FUNCTION_INFO_V1(kadb_offsets_to_timestamp);
PG_FUNCTION_INFO_V1(kadb_offsets_to_earliest);
PG_FUNCTION_INFO_V1(kadb_offsets_to_latest);
PG_FUNCTION_INFO_V1(kadb_offsets_to_committed);
