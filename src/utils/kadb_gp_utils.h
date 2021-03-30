#ifndef KADB_FDW_KADB_GP_UTILS_INCLUDED
#define KADB_FDW_KADB_GP_UTILS_INCLUDED

/*
 * Common reusable utilities to interact with GPDB
 */

#include <postgres.h>


/**
 * A wrapper for 'AllocSetContextCreate()', which uses 'uoid' to generate a
 * (unique) name for the memory context allocated.
 */
MemoryContext allocate_temporary_context_with_unique_name(const char *prefix, Oid uoid);


#endif   /* // KADB_FDW_KADB_GP_UTILS_INCLUDED */
