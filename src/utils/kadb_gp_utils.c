#include "kadb_gp_utils.h"

#include <cdb/cdbutil.h>
#include <utils/memutils.h>


MemoryContext
allocate_temporary_context_with_unique_name(const char *prefix, Oid uoid)
{
	Assert(PointerIsValid(prefix));

	StringInfoData temporary_insert_context_name;

	initStringInfo(&temporary_insert_context_name);
	appendStringInfo(&temporary_insert_context_name, "kadb_temporary_context_%s_%u", prefix, uoid);
	MemoryContext result = AllocSetContextCreate(CurrentMemoryContext,
										  temporary_insert_context_name.data,
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	pfree(temporary_insert_context_name.data);
	return result;
}
