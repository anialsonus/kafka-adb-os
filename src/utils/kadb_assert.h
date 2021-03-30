#ifndef KADB_FDW_UTILS_KADB_ASSERT_INCLUDED
#define KADB_FDW_UTILS_KADB_ASSERT_INCLUDED

/*
 * Assert aliases.
 */

#include <postgres.h>

#include <cdb/cdbvars.h>


/**
 * Assert the current process is the controller's process.
 */
#define ASSERT_CONTROLLER() Assert(Gp_role == GP_ROLE_DISPATCH)

/**
 * Assert the current process is some executor's process.
 */
#define ASSERT_EXECUTOR() Assert(Gp_role == GP_ROLE_EXECUTE)


#endif   /* KADB_FDW_UTILS_KADB_ASSERT_INCLUDED */
