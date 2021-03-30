#ifndef KADB_FDW_PLANNING_INCLUDED
#define KADB_FDW_PLANNING_INCLUDED

/*
 * Planning methods for Kafka-ADB.
 */

#include <postgres.h>

#include <foreign/fdwapi.h>


/**
 * FDW interface function.
 *
 * Sets size and cost of a foreign scan.
 *
 * This method also connects to Kafka (checking the connection parameters are
 * correct), retrieves a list of available partitions, forms a distribution of
 * these partitions across GPDB segments, and serializes these data as a list
 * of nodes.
 */
void		kadbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

/**
 * FDW interface function.
 *
 * Sets scan paths of a foreign scan.
 */
void		kadbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

/**
 * FDW interface function.
 *
 * Converts planner data into a 'ForeignScan' node which is used during
 * execution.
 */
ForeignScan *kadbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses);


#endif   /* KADB_FDW_PLANNING_INCLUDED */
