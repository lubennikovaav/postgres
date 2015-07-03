#include "postgres.h"

#include "access/genam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"
#include "access/hash.h"
#include "cola.h"

PG_MODULE_MAGIC;

/*
 * colaBuildCallback
 */
static void
colaBuildCallback(Relation index,
				HeapTuple htup,
				Datum *values,
				bool *isnull,
				bool tupleIsAlive,
				void *state)
{
	ColaInsertState	*insstate = (ColaInsertState *) state;
	ColaState *colastate = insstate->colastate;

	MemoryContext	oldCtx;
	IndexTuple		itup;
	bool			result;

	//oldCtx = MemoryContextSwitchTo(insstate->colastate->tempCxt);

	insstate = initColaInsertState(index);
	fillColaInsertState(insstate);
	itup = index_form_tuple(colastate->tupdesc, values, isnull); 
    itup->t_tid = htup->t_self;
	result = _cola_doinsert(index, insstate, itup);

    //MemoryContextSwitchTo(oldCtx);
    //MemoryContextReset(insstate->colastate->tempCxt);
}

/*
 * colabuild
 */

PG_FUNCTION_INFO_V1(colabuild);
Datum       colabuild(PG_FUNCTION_ARGS);

Datum
colabuild(PG_FUNCTION_ARGS)
{

	Relation    heap = (Relation) PG_GETARG_POINTER(0);
	Relation    index = (Relation) PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);

    IndexBuildResult *result;
    double      reltuples;
	ColaInsertState *state;
    Buffer      metabuffer;

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			RelationGetRelationName(index));

    /* initialize the meta page */
	metabuffer = ColaGetBuffer(index, P_NEW);

	START_CRIT_SECTION();
	ColaInitMetaBuffer(metabuffer);
	MarkBufferDirty(metabuffer);
	UnlockReleaseBuffer(metabuffer);
	END_CRIT_SECTION();

	state = initColaInsertState(index);
	/*
	 * Create a temporary memory context (state->colastate->tempCxt) that is reset once for each tuple
	 * processed.
	 */
	state->colastate->tempCxt = createTempColaContext();
    /*
     * Do the heap scan.
     */
    reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
                                   colaBuildCallback, (void *) state);

	/*if (state->currentBuffer != InvalidBuffer)
	{
		MarkBufferDirty(state->currentBuffer);
		UnlockReleaseBuffer(state->currentBuffer);
	}*/

    /* okay, all heap tuples are indexed */
    MemoryContextDelete(state->colastate->tempCxt);
    freeColaState(state->colastate);

    /*
	 * Return statistics
	 */
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = result->index_tuples = reltuples;

    PG_RETURN_POINTER(result);
}

/*
 * colabuildempty. Is not supported yet.
 */

PG_FUNCTION_INFO_V1(colabuildempty);
Datum       colabuildempty(PG_FUNCTION_ARGS);
Datum
colabuildempty(PG_FUNCTION_ARGS)
{
	//elog(NOTICE, "Debug. colabuildempty");
	PG_RETURN_VOID();
}

/* Try to insert index tuple at level 0 */
bool
ColaTryInsert(Relation index, ColaInsertState *state, IndexTuple itup) {
    BlockNumber blkno = InvalidBlockNumber;
    Buffer buf;
    uint16 arrToInsert;
    int levelToInsert, arrnumToInsert;

    for (;;) {
		arrToInsert = ColaFindArray(0, (state->ColaArrayState));
		levelToInsert = A_LEVEL(arrToInsert);
		arrnumToInsert =  A_ARRNUM(arrToInsert);

		if (arrToInsert == InvalidColaArrayState)
			return false;

		if (!A_ISEXIST(arrToInsert))
			blkno = P_NEW;
		else
			blkno = ColaGetBlkno(levelToInsert, arrnumToInsert, 0);

		buf = ColaGetBuffer(index, blkno);

		if(!ColaPageAddItem(buf, itup, state, levelToInsert)) {
			UnlockReleaseBuffer(buf);
		    /*
			 * Insertion failed. It means there is no place at the page.
			 * So change array state to full and try to open another array of level 0.
			 */
			arrToInsert |= CAS_FULL;
			state->ColaArrayState[levelToInsert][arrnumToInsert] = arrToInsert;
			saveColaInsertState(state);
		}
		else {
			UnlockReleaseBuffer(buf);
			/*
			 * Index tuple is inserted into the array at level 0.
			 * Now array contains data, change state to exists and visible if necessary.
			 */
			if ((!A_ISEXIST(arrToInsert))||(!A_ISVISIBLE(arrToInsert))) {
				arrToInsert |= CAS_EXISTS | CAS_VISIBLE;
				state->ColaArrayState[levelToInsert][arrnumToInsert] = arrToInsert;
				saveColaInsertState(state);
			}
		    return true;
		}
	}
}

/* 
 * According to COLA algorithm, always insert data at level 0.
 * If level 0 is unsafe (contains 2 FULL arrays) recursively merge down into next level.
 * After the merge, level 0 will be safe (it doesn't contain 2 FULL arrays).
 * Insert new data at level 0.
 *
 * In _cola_doinsert():
 * 1. Try to insert index tuple at level 0.
 * 2. If data is not inserted, call merge from level 0 to level 1
 * 3. If merge_0to1 failed, call recursive merge down from level 1
 * 4. Call merge_0to1 again
 * 5. Try to insert data at level 0 again
 */
bool
_cola_doinsert(Relation index, ColaInsertState *state, IndexTuple itup) {
    if(!ColaTryInsert(index, state, itup)) {
        if(!_cola_merge_0to1(index, state)) {
			if(!_cola_merge(index, state)) {
				elog(LOG, "COLA index. _cola_merge failed.");
				return false;
			}
			if(!_cola_merge_0to1(index, state)) {
				elog(LOG, "COLA index. _cola_doinsert. _cola_merge_0to1 failed twice");
				return false;
			}
		}
		if(!ColaTryInsert(index, state, itup)) {
			elog(LOG, "COLA index. _cola_doinsert. ColaTryInsert failed twice");
			return false;
		}
    }
	return true;
}


/*
 * colainsert.
 */
PG_FUNCTION_INFO_V1(colainsert);
Datum       colainsert(PG_FUNCTION_ARGS);
Datum
colainsert(PG_FUNCTION_ARGS)
{
	Relation    index = (Relation) PG_GETARG_POINTER(0);
	Datum      *values = (Datum *) PG_GETARG_POINTER(1);
	bool       *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer ht_ctid = (ItemPointer) PG_GETARG_POINTER(3);
	IndexTuple  itup;
	bool		result;

	ColaInsertState* state = (ColaInsertState*) initColaInsertState(index);
	fillColaInsertState(state);

    /* generate an index tuple */
    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    itup->t_tid = *ht_ctid;

    result = _cola_doinsert(index, state, itup);

    pfree(itup);
    freeColaState(state->colastate);
	pfree(state);

    PG_RETURN_BOOL(result);
}
