#include "postgres.h"

#include "access/genam.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "cola.h"

PG_FUNCTION_INFO_V1(colabulkdelete);
Datum       colabulkdelete(PG_FUNCTION_ARGS);
Datum
colabulkdelete(PG_FUNCTION_ARGS)
{
	elog(ERROR, "COLA does not support any vacuum. From colabulkdelete().");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(colavacuumcleanup);
Datum       colavacuumcleanup(PG_FUNCTION_ARGS);
Datum
colavacuumcleanup(PG_FUNCTION_ARGS)
{
	elog(ERROR, "COLA does not support any vacuum. From colavacuumcleanup().");
	PG_RETURN_VOID();
}
