/*-------------------------------------------------------------------------
 *
 * amcmds.c
 *	  Routines for SQL commands that manipulate access methods.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/amcmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

typedef enum
{
	AMP_SMALLINT,
	AMP_BOOLEAN,
	AMP_TYPE,
	AMP_PROC
} AMParamType;

typedef struct
{
	OffsetNumber	attnum;
	const char	   *name;
	AMParamType		type;
	int				nargs;
	Oid			    argTypes[7];
	Oid				returnType;
	bool			required;
} AMParamDef;

static AMParamDef paramsDef[] = {
	{
		Anum_pg_am_amstrategies,
		"nstrategies",
		AMP_SMALLINT,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amsupport,
		"nsupport",
		AMP_SMALLINT,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amcanorder,
		"canorder",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amcanorderbyop,
		"canorderbyop",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amcanbackward,
		"canbackward",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amcanunique,
		"canunique",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amcanmulticol,
		"canmulticol",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amoptionalkey,
		"optionalkey",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amsearcharray,
		"searcharray",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amsearchnulls,
		"searchnulls",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amstorage,
		"storage",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amclusterable,
		"clusterable",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_ampredlocks,
		"predlocks",
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_amkeytype,
		"keytype",
		AMP_TYPE,
		-1,
		{},
		InvalidOid,
		true
	},
	{
		Anum_pg_am_aminsert,
		"insert",
		AMP_PROC,
		6,
		{INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID},
		BOOLOID,
		true
	},
	{
		Anum_pg_am_ambeginscan,
		"beginscan",
		AMP_PROC,
		3,
		{INTERNALOID, INT4OID, INT4OID},
		INTERNALOID,
		true
	},
	{
		Anum_pg_am_amgettuple,
		"gettuple",
		AMP_PROC,
		2,
		{INTERNALOID, INTERNALOID},
		BOOLOID,
		false
	},
	{
		Anum_pg_am_amgetbitmap,
		"getbitmap",
		AMP_PROC,
		2,
		{INTERNALOID, INTERNALOID},
		INT8OID,
		true
	},
	{
		Anum_pg_am_amrescan,
		"rescan",
		AMP_PROC,
		5,
		{INTERNALOID, INTERNALOID, INT4OID, INTERNALOID, INT4OID},
		VOIDOID,
		true
	},
	{
		Anum_pg_am_amendscan,
		"endscan",
		AMP_PROC,
		1,
		{INTERNALOID},
		VOIDOID,
		true
	},
	{
		Anum_pg_am_ammarkpos,
		"markpos",
		AMP_PROC,
		1,
		{INTERNALOID},
		VOIDOID,
		true
	},
	{
		Anum_pg_am_amrestrpos,
		"restrpos",
		AMP_PROC,
		1,
		{INTERNALOID},
		VOIDOID,
		true
	},
	{
		Anum_pg_am_ambuild,
		"build",
		AMP_PROC,
		3,
		{INTERNALOID, INTERNALOID, INTERNALOID},
		INTERNALOID,
		true
	},
	{
		Anum_pg_am_ambuildempty,
		"buildempty",
		AMP_PROC,
		1,
		{INTERNALOID},
		VOIDOID,
		true
	},
	{
		Anum_pg_am_ambulkdelete,
		"bulkdelete",
		AMP_PROC,
		4,
		{INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID},
		INTERNALOID,
		true
	},
	{
		Anum_pg_am_amvacuumcleanup,
		"vacuumcleanup",
		AMP_PROC,
		2,
		{INTERNALOID, INTERNALOID},
		INTERNALOID,
		true
	},
	{
		Anum_pg_am_amcanreturn,
		"canreturn",
		AMP_PROC,
		1,
		{INTERNALOID},
		BOOLOID,
		false
	},
	{
		Anum_pg_am_amcostestimate,
		"costestimate",
		AMP_PROC,
		7,
		{INTERNALOID, INTERNALOID, FLOAT8OID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID},
		VOIDOID,
		false
	},
	{
		Anum_pg_am_amoptions,
		"options",
		AMP_PROC,
		2,
		{TEXTARRAYOID, BOOLOID},
		BYTEAOID,
		false
	},
	{
		InvalidOffsetNumber,
		NULL,
		AMP_BOOLEAN,
		-1,
		{},
		InvalidOid,
		false
	}
};

static char *
getAMName(List *names)
{
	char	*amname, *catalogname;

	switch (list_length(names))
	{
		case 1:
			amname = strVal(linitial(names));
			break;
		case 2:
			catalogname = strVal(linitial(names));
			amname = strVal(lthird(names));

			/*
			 * We check the catalog name and then ignore it.
			 */
			if (strcmp(catalogname, get_database_name(MyDatabaseId)) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("cross-database references are not implemented: %s",
						 NameListToString(names))));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("improper access method name (too many dotted names): %s",
					   NameListToString(names))));
			break;
	}
	return amname;
}

/*
 * DefineAccessMethod
 *		Registers a new access method.
 */
Oid
DefineAccessMethod(List *names, List *parameters)
{
	ListCell   *pl;
	bool		nulls[Natts_pg_am];
	Datum		values[Natts_pg_am];
	AMParamDef *paramDef;
	char	   *amname;
	Oid			amoid;
	NameData	name;
	Relation	pg_am_desc;
	HeapTuple	tup;
	int			i;
	ObjectAddress myself, referenced;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create access methods")));

	/* Check if name is busy */
	amname = getAMName(names);
	amoid = GetSysCacheOid1(AMNAME, CStringGetDatum(amname));
	if (OidIsValid(amoid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("access method \"%s\" already exists", amname)));
	}

	/* Fill access method tuple */
	for (i = 0; i < Natts_pg_am; i++)
		nulls[i] = true;

	namestrcpy(&name, amname);
	nulls[Anum_pg_am_amname - 1] = false;
	values[Anum_pg_am_amname - 1] = NameGetDatum(&name);

	/* Extract the parameters from the parameter list */
	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		bool found = false;

		paramDef = paramsDef;
		while (paramDef->attnum != InvalidOffsetNumber)
		{
			Oid	funcOid, typeId;

			if (pg_strcasecmp(defel->defname, paramDef->name) != 0)
			{
				paramDef++;
				continue;
			}

			switch (paramDef->type)
			{
				case AMP_SMALLINT:
					values[paramDef->attnum - 1] = Int32GetDatum(defGetInt32(defel));
					break;
				case AMP_BOOLEAN:
					values[paramDef->attnum - 1] = BoolGetDatum(defGetBoolean(defel));
					break;
				case AMP_TYPE:
					typeId = typenameTypeId(NULL, defGetTypeName(defel));
					values[paramDef->attnum - 1] = ObjectIdGetDatum(typeId);
					break;
				case AMP_PROC:
					funcOid = LookupFuncName(defGetQualifiedName(defel), paramDef->nargs, paramDef->argTypes, false);
					values[paramDef->attnum - 1] = ObjectIdGetDatum(funcOid);
					break;
				default:
					elog(ERROR, "Unknown AM parameter type: %d", paramDef->type);
					break;
			}
			nulls[paramDef->attnum - 1] = false;
			found = true;
			break;
		}
		if (!found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("access method attribute \"%s\" not recognized",
							defel->defname)));
		}
	}

	/* Check if required attributes are filled */
	paramDef = paramsDef;
	while (paramDef->attnum != InvalidOffsetNumber)
	{
		if (nulls[paramDef->attnum - 1])
		{
			if (paramDef->required)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("access method attribute \"%s\" is required",
								 paramDef->name)));
			}
			else
			{
				Assert(paramDef->type == AMP_PROC);
				nulls[paramDef->attnum - 1] = false;
				values[paramDef->attnum - 1] = ObjectIdGetDatum(0);
			}
		}
		paramDef++;
	}

	pg_am_desc = heap_open(AccessMethodRelationId, RowExclusiveLock);

	tup = heap_form_tuple(RelationGetDescr(pg_am_desc), values, nulls);
	amoid = simple_heap_insert(pg_am_desc, tup);
	CatalogUpdateIndexes(pg_am_desc, tup);
	heap_freetuple(tup);

	myself.classId = AccessMethodRelationId;
	myself.objectId = amoid;
	myself.objectSubId = 0;

	paramDef = paramsDef;
	while (paramDef->attnum != InvalidOffsetNumber)
	{
		if (paramDef->type == AMP_TYPE || paramDef->type == AMP_PROC)
		{
			Oid objectId = DatumGetObjectId(values[paramDef->attnum - 1]);
			if (OidIsValid(objectId))
			{
				referenced.classId = (paramDef->type == AMP_TYPE) ? TypeRelationId : ProcedureRelationId;
				referenced.objectId = objectId;
				referenced.objectSubId = 0;
				recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
			}
		}
		paramDef++;
	}
	recordDependencyOnCurrentExtension(&myself, false);

	heap_close(pg_am_desc, RowExclusiveLock);

	return InvalidOid;
}

/*
 * Guts of access method deletion.
 */
void
RemoveAccessMethodById(Oid amOid)
{
	Relation	relation;
	HeapTuple	tup;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop access methods")));

	relation = heap_open(AccessMethodRelationId, RowExclusiveLock);

	tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for access method %u", amOid);

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}
