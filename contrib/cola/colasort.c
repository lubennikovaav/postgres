#include "postgres.h"

#include "miscadmin.h"
#include "utils/tuplesort.h"

#include "cola.h"

/*
 * create and initialize a spool structure
 */
ColaSpool*
_cola_spoolinit(Relation index, TupleDesc tupdesc)
{
	//elog(NOTICE, "Debug._cola_spoolinit.");
	ColaSpool	   *colaspool = (ColaSpool *) palloc0(sizeof(ColaSpool));
	colaspool->index = index;
	colaspool->tupdesc = tupdesc;
	/*
	 * We size the sort area as maintenance_work_mem rather than work_mem to
	 * speed index creation.  This should be OK since a single backend can't
	 * run multiple index creations in parallel.
	 */
	colaspool->sortstate = tuplesort_begin_index_cola(index,
												   maintenance_work_mem,
												   false);
	return colaspool;
}

/*
 * clean up a spool structure and its substructures.
 */
void
_cola_spooldestroy(ColaSpool *colaspool)
{
	//elog(NOTICE, "Debug._cola_spooldestroy.");
	tuplesort_end(colaspool->sortstate);
	pfree(colaspool);
}

/*
 * spool an index entry into the sort file.

void
_cola_spool(ColaSpool *colaspool, ItemPointer self, Datum *values, bool *isnull)
{
	elog(NOTICE, "Debug._cola_spool.");
	tuplesort_putindextuplevalues(colaspool->sortstate, colaspool->index,
								  self, values, isnull);
}
 */

/*
 * spool an index tuple into the sort file.
 */
void
_cola_spool_tuple(ColaSpool *colaspool, IndexTuple itup)
{
	//elog(NOTICE, "Debug._cola_spool_tuple.");
	Datum values[INDEX_MAX_KEYS];
	bool isnull[INDEX_MAX_KEYS];
		bool should_free;
		Datum dat_debug;
	int i;
	for (i=0; i<INDEX_MAX_KEYS;i++) {
		isnull[i] = false;
		values[i] = Int32GetDatum(0);
	}

		dat_debug = index_getattr(itup, 1, colaspool->tupdesc, &should_free);
		//elog(NOTICE, "Debug. Item[%d] add to spool %d",i, DatumGetInt32(dat_debug));
		index_deform_tuple(itup, colaspool->tupdesc, values, isnull);

		//elog(NOTICE, "Debug._cola_spool_tuple.1");


	tuplesort_putindextuplevalues(colaspool->sortstate, colaspool->index,
							 &(itup->t_tid), values, isnull);
}
