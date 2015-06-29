#include "postgres.h"

#include <math.h>
#include "catalog/index.h"
#include "storage/lmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "access/reloptions.h"
#include "access/itup.h"
#include "storage/freespace.h"

#include "cola.h"

/*
 * colaoptions TODO
 */
PG_FUNCTION_INFO_V1(colaoptions);
Datum       colaoptions(PG_FUNCTION_ARGS);
Datum
colaoptions(PG_FUNCTION_ARGS)
{
	elog(ERROR, "COLA does not have colaoptions.");
	PG_RETURN_VOID();
};

/*
 * initColaState
 */
ColaState*
initColaState(Relation index)
{
	ColaState  *colastate;
	MemoryContext scanCxt;
	MemoryContext oldCxt;
	int         i;

	/* safety check */
	if (index->rd_att->natts > INDEX_MAX_KEYS)
		elog(ERROR, "numberOfAttributes %d > %d",
			index->rd_att->natts, INDEX_MAX_KEYS);

    /* Create the memory context that will hold the ColaState */
    scanCxt = AllocSetContextCreate(CurrentMemoryContext,
                                    "COLA scan context",
                                    ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_MAXSIZE);
    oldCxt = MemoryContextSwitchTo(scanCxt);

	/* Create and fill in the ColaState */
	colastate = (ColaState *) palloc(sizeof(ColaState));
	colastate->scanCxt = scanCxt;
    colastate->tempCxt = scanCxt;       /* caller must change this if needed */
    colastate->tupdesc = index->rd_att;

    for (i = 0; i <index->rd_att->natts; i++)
        fmgr_info_copy(&(colastate->orderFn[i]),
                       index_getprocinfo(index, i+1, COLAORDER_PROC),
                       scanCxt);

    MemoryContextSwitchTo(oldCxt);
	return colastate;
}

/*
 * freeColaState.
 */
void
freeColaState(ColaState *colastate)
{
    /* It's sufficient to delete the scanCxt */
    MemoryContextDelete(colastate->scanCxt);
}

/*
 * createTempColaContext
 */
MemoryContext
createTempColaContext(void)
{
    return AllocSetContextCreate(CurrentMemoryContext,
                                 "COLA temporary context",
                                 ALLOCSET_DEFAULT_MINSIZE,
                                 ALLOCSET_DEFAULT_INITSIZE,
                                 ALLOCSET_DEFAULT_MAXSIZE);
}

ColaInsertState*
initColaInsertState(Relation index) {
	ColaInsertState *state = (ColaInsertState *)palloc0(sizeof(ColaInsertState));
	state->colastate = initColaState(index);
	state->indexrel = index;
	state->needPage = false;
	state->currentCell = 0;
	state->currentBuffer = InvalidBuffer;

	/* RLP information is unnecessary there */
	state->lastMerge = false;
	state->nNewRLPs = state->curNewRLPs = 0;
	state->newRLPs = NULL;

	memset(state->ColaArrayState, 0, MaxColaHeight*3*sizeof(uint16));
	return state;
}

/*
 * Read information about COLA array states from metapage.
 * Must be called after any changes in ColaArrayState done by other process.
 */
void
fillColaInsertState(ColaInsertState *state) {
 	//elog(NOTICE, "Debug. fillColaInsertState");
	Buffer b = ColaGetBuffer(state->indexrel, COLA_METAPAGE);
	Page				page = BufferGetPage(b);
	ColaMetaPageData	*metadata = ColaPageGetMeta(page);

	/* copy ColaArrayState from metapage into ColaInsertState */
	memcpy(state->ColaArrayState, metadata->ColaArrayState, MaxColaHeight*3*sizeof(uint16));
	
	UnlockReleaseBuffer(b);
}

void
fillColaScanOpaque(COLAScanOpaque so) {
	Buffer b = ColaGetBuffer(so->indexrel, COLA_METAPAGE);
	Page				page = BufferGetPage(b);
	ColaMetaPageData	*metadata = ColaPageGetMeta(page);

	//copy metadata into COLAScanOpaque
	memcpy(so->ColaArrayState, metadata->ColaArrayState, MaxColaHeight*3*sizeof(uint16));
	UnlockReleaseBuffer(b);

}

/* save metadata from ColaInsertState to metapage */
ColaInsertState*
saveColaInsertState(ColaInsertState *state) {
	Buffer b = ColaGetBuffer(state->indexrel, COLA_METAPAGE);
	Page				page = BufferGetPage(b);
	ColaMetaPageData	*metadata = ColaPageGetMeta(page);

	START_CRIT_SECTION();
	memcpy(metadata->ColaArrayState,state->ColaArrayState, MaxColaHeight*3*sizeof(uint16));
	MarkBufferDirty(b);
	UnlockReleaseBuffer(b);
	END_CRIT_SECTION();

	return state;
}

/*
 * ColaInitPage
 */
void
ColaInitPage(Page page, Size size)
{
    //ColaPageOpaque opaque;
    PageInit(page, size, sizeof(ColaPageOpaqueData));

	//opaque = ColaPageGetOpaque(page);
	//memset(opaque, 0, sizeof(ColaPageOpaqueData));
	//opaque->flags = 0; // flags: COLA_META, COLA_DELETED 
}


/*
 * ColaInitMetaBuffer
 */
void
ColaInitMetaBuffer(Buffer buf)
{
	ColaMetaPageData	*metadata;
	Page				page = BufferGetPage(buf);
	int level, arrnum;

	ColaInitPage(page, BufferGetPageSize(buf));

	metadata = ColaPageGetMeta(page);
	memset(metadata, 0, sizeof(ColaMetaPageData));

	metadata->cola_magic = COLA_MAGIC;

	memset(metadata->ColaArrayState, 0, MaxColaHeight*3*sizeof(uint16));

	/* set default states for all cola arrays */
	for (level = 0; level < MaxColaHeight; level++) {
		for (arrnum=0; arrnum<3; arrnum++) {
			metadata->ColaArrayState[level][arrnum] = 0;
			metadata->ColaArrayState[level][arrnum] |= (level << 7) | (arrnum << 5);
		}
	}
}

/*
 * Calculate the BlockNumber of the COLA array's cell
 */
BlockNumber
ColaGetBlkno(int level, int arrnum, int cellnum) {

    BlockNumber blkno;
	int i;

	if (level == 0)
		return (arrnum + 1);
	else
		blkno = 3;

	if (cellnum > (int) pow(2.0, (double)(level)))
		elog(ERROR, "No cell %d at level %d", cellnum, level);

	for (i = 1; i < level; i++) {
		blkno += 3 * (int) pow(2.0, (double)(i));
	}

	blkno += arrnum * (int) pow(2.0, (double)(level));
	blkno += cellnum;

    return blkno;
}

/*
 * Get a buffer by block number for read or write TODO delete/uncomment unnesessary code
 */
Buffer 
ColaGetBuffer(Relation index, BlockNumber blkno)
{
    Buffer      buf;
	int access = BUFFER_LOCK_EXCLUSIVE;
    if (blkno != P_NEW)
    {
        /* Read an existing block of the relation */
        buf = ReadBuffer(index, blkno);
        LockBuffer(buf, access);
    }
    else
    {
		Page page;
        /* Extend the relation by one page. */

         /* We have to use a lock to ensure no one else is extending the rel at
         * the same time, else we will both try to initialize the same new
         * page.  We can skip locking for new or temp relations, however,
         * since no one else could be accessing them.
        
        needLock = !RELATION_IS_LOCAL(index);
        if (needLock)
            LockRelationForExtension(index, ExclusiveLock);
         */

		buf = ReadBuffer(index, P_NEW);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * Release the file-extension lock; it's now OK for someone else to
         * extend the relation some more.  Note that we cannot release this
         * lock before we have buffer lock on the new page, or we risk a race
         * condition against btvacuumscan --- see comments therein.
         
        if (needLock)
            UnlockRelationForExtension(index, ExclusiveLock);
        */

        /* Initialize the new page before returning it */
        page = BufferGetPage(buf);
        Assert(PageIsNew(page));
        ColaInitPage(page, BufferGetPageSize(buf));
    }

    return buf;
}

uint16 ColaFindArray(int level, uint16 cas[MaxColaHeight][3]) {
	int arrnum;
	int max_arrnum =3;
	//we always have only two arrays at level 0
	if (level == 0)
		max_arrnum =2;

	/* look for exist visible empty array */
	for(arrnum = 0; arrnum < max_arrnum; arrnum++) {
		if ((!A_ISMERGE(cas[level][arrnum]))&&(!A_ISFULL(cas[level][arrnum]))&&(A_ISVISIBLE(cas[level][arrnum])))
			return cas[level][arrnum];
	}

	/* look for exist not_visible empty array */
	for(arrnum = 0; arrnum < max_arrnum; arrnum++) {
		if ((!A_ISMERGE(cas[level][arrnum]))&&(!A_ISFULL(cas[level][arrnum]))&&(!A_ISVISIBLE(cas[level][arrnum]))&&(A_ISEXIST(cas[level][arrnum])))
			return cas[level][arrnum];
	}

	/* look for not_exist array */
	for(arrnum = 0; arrnum < max_arrnum; arrnum++) {
		if ((!A_ISMERGE(cas[level][arrnum]))&&(!A_ISFULL(cas[level][arrnum]))&&(!A_ISEXIST(cas[level][arrnum])))
			return cas[level][arrnum];
	}
	
	/* If there's no empty array at the level, return InvalidColaArrayState. */
	return InvalidColaArrayState;
}

/* It was new page. First tuple is inserted. 
 * Save it into newRLPs.
 */
void ColaSaveRLP(Buffer buf, IndexTuple itup, ColaInsertState *state) {
	IndexTuple new_item;
	new_item = CopyIndexTuple(itup);
	ItemPointerSet(&(new_item->t_tid), BufferGetBlockNumber(buf), ColaRLPOffset);
	state->newRLPs[state->curNewRLPs++] = new_item;
}

/*
 * ColaPageAddItem TODO fix SizeOfRLP
 */
bool
ColaPageAddItem(Buffer buf, IndexTuple itup, ColaInsertState *state, int levelToInsert)                          
{
    Page    p = BufferGetPage(buf);
	int spaceForRLPs = 0;
    Size    itemsize = IndexTupleDSize(*itup);
	itemsize = MAXALIGN(itemsize);

	/* Should save free space for 2 RLPs at each page. Because of merging down at level that contains RLP we should save this place. */
	if (levelToInsert == 0)
		spaceForRLPs = (SizeOfRLP+4)*2; //TODO:SizeOfRLP should be computed by known size of datatype


    if (PageGetFreeSpace(p) < itemsize + spaceForRLPs)
        return false;

    if (PageAddItem(p, (Item) itup, itemsize, InvalidOffsetNumber,
                    false, false) == InvalidOffsetNumber)
        return false;

	MarkBufferDirty(buf);

	if ((PageGetMaxOffsetNumber(p) == FirstOffsetNumber)&&(state->lastMerge))
		ColaSaveRLP(buf, itup, state);

    return true;
}

bool
LevelIsSafe(int level, ColaInsertState *state) {
	uint16 	arrToCheck;
	int 	arrnum, count = 0;
	int 	max_arrnum = 3;

	if (level == 0)
		max_arrnum = 2;

	/* Level is unsafe is there are 2 FULL arrays at the level */
	for(arrnum = 0; arrnum < max_arrnum; arrnum++) {
		arrToCheck = state->ColaArrayState[level][arrnum];
		if ((A_ISFULL(arrToCheck))&&(A_ISVISIBLE(arrToCheck)))
			count++;
		if (A_ISMERGE(arrToCheck))
			count++;
	}

	return (count < 2);
}
