#ifndef KADB_FDW_EXECUTION_INCLUDED
#define KADB_FDW_EXECUTION_INCLUDED

/*
 * Execution methods for Kafka-ADB FDW
 */

#include <postgres.h>

#include <foreign/fdwapi.h>


/**
 * FDW interface function.
 *
 * Begins a foreign scan.
 */
void		kadbBeginForeignScan(ForeignScanState *node, int eflags);

/**
 * FDW interface function.
 *
 * Returns a tuple during a foreign scan or signalises a finish of the scan.
 */
TupleTableSlot *kadbIterateForeignScan(ForeignScanState *node);

/**
 * FDW interface function.
 *
 * Restarts a foreign scan.
 */
void		kadbReScanForeignScan(ForeignScanState *node);

/**
 * FDW interface function.
 *
 * Ends a foreign scan.
 */
void		kadbEndForeignScan(ForeignScanState *node);


#endif   /* // KADB_FDW_EXECUTION_INCLUDED */
