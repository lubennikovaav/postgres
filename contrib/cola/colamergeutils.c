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

void
ColaAddPageToSpool(Relation index, ColaSpool *spool, BlockNumber blkno) {
	OffsetNumber 	i, maxoff;
	IndexTuple  	item;
	Buffer buf = ColaGetBuffer(index, blkno);
	Page page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {

	    item = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
	    _cola_spool_tuple(spool, item);
	}

	UnlockReleaseBuffer(buf);
}


bool
ColaInsert(Relation index, ColaInsertState *insstate, IndexTuple itup, int arrToInsert) {
    BlockNumber blkno = InvalidBlockNumber;
	int levelToInsert = A_LEVEL(arrToInsert);
	int arrnumToInsert =  A_ARRNUM(arrToInsert);
	int cellMax = (int) pow(2.0, (double)(levelToInsert));

    for (;;) {
		if (insstate->needPage) {
			blkno = P_NEW;
			insstate->needPage = false;
		}
		else {
			blkno = ColaGetBlkno(levelToInsert, arrnumToInsert, insstate->currentCell);
		}

		insstate->currentBuffer = ColaGetBuffer(index, blkno);

		if(ColaPageAddItem(insstate->currentBuffer, itup, insstate, levelToInsert)) {
			UnlockReleaseBuffer(insstate->currentBuffer);
		    return true;
		}
		else {
			UnlockReleaseBuffer(insstate->currentBuffer);

		    if (insstate->currentCell < cellMax) {
				insstate->currentCell++;
				
				if (!A_ISEXIST(arrToInsert)) {
					insstate->needPage = true;
				}
			}
		    else {
				/* If we are here, it means that we filled all cells of arrToInsert and trying to continue insertions. */
		    	elog(ERROR, "COLA index. ColaInsert. insstate->currentCell %d >= cellMax %d. Shouldn't be there.",insstate->currentCell,cellMax );
			}
		}
	}
}

void
ColaSaveOldRLPs(Relation index, uint16 arr, ColaInsertState *state, ColaSpool *spool) {
	BlockNumber blkno;
	Buffer buffer;
	Page page;
	OffsetNumber maxoff;
	MemoryContext oldCxt;
	ColaState *colastate = state->colastate;
	int i;
	int cell = 0;
	int maxCell = (int) pow(2.0, (double)(A_LEVEL(arr)));

	state->nOldRLPs = (int) pow(2.0, (double)(A_LEVEL(arr)+1));

	if(A_LEVEL(arr) != 1) {
    	oldCxt = MemoryContextSwitchTo(colastate->scanCxt);
		state->oldRLPs = palloc(sizeof(IndexTuple)*state->nOldRLPs);
		MemoryContextSwitchTo(oldCxt);
	}
	state->curOldRLPs = 0;

	/* Read oldRLPs from arrToInsert. Do cycle, while page has items (RLPs) */
	do {
		blkno = ColaGetBlkno(A_LEVEL(arr), A_ARRNUM(arr), cell);
		buffer = ColaGetBuffer(index, blkno);
		page = BufferGetPage(buffer);
		maxoff = PageGetMaxOffsetNumber(page);

		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
			if(A_LEVEL(arr) != 1) {
				IndexTuple it = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
				/* Switch to memory context where oldRLPs will be saved while level is merging */
    			oldCxt = MemoryContextSwitchTo(colastate->scanCxt);

				state->oldRLPs[state->curOldRLPs] = CopyIndexTuple (it);
				ItemPointerSet(&((state->oldRLPs[state->curOldRLPs])->t_tid), BlockIdGetBlockNumber(&(it->t_tid.ip_blkid)), ColaRLPOffset);

    			MemoryContextSwitchTo(oldCxt);
			}
			else
				_cola_spool_tuple(spool, (IndexTuple) PageGetItem(page, PageGetItemId(page, i)));
			state->curOldRLPs++;
		}
		UnlockReleaseBuffer(buffer);
		ColaClearPage(index, blkno);

		cell++;
	} while ((maxoff > 0)&&(cell < maxCell));

	state->nOldRLPs = state->curOldRLPs;
}

bool
_cola_merge_0to1(Relation index, ColaInsertState *state) {
	ColaState *colastate = state->colastate;
    ColaSpool *spool;
    IndexTuple itup;
    bool inserted = true;
    bool should_free;
	uint16 arrToInsert;
	int levelToInsert = 1;
	int arrnumToInsert;

	fillColaInsertState(state);

	/* It seem to be unnecessary check. 
	Because ColaFindArray on unsafe level must return InvalidState
	*/
	if (!LevelIsSafe(levelToInsert, state))
		return false;

	arrToInsert = ColaFindArray(1, state->ColaArrayState);

	if (arrToInsert == InvalidColaArrayState)
		return false;

	arrnumToInsert =  A_ARRNUM(arrToInsert);
	state->needPage = !A_ISEXIST(arrToInsert);

	//elog(NOTICE, "_cola_merge_0to1, arrToInsert %d ", arrToInsert);

	/* Set MERGE flag for arrays */
	state->ColaArrayState[0][0] |= CAS_MERGE;
	state->ColaArrayState[0][1] |= CAS_MERGE;
	arrToInsert |= CAS_MERGE;
	state->ColaArrayState[levelToInsert][arrnumToInsert] = arrToInsert;
	saveColaInsertState(state);

	spool = _cola_spoolinit(index, colastate->tupdesc);
    ColaAddPageToSpool(index, spool, ColaGetBlkno(0,0,0));
    ColaAddPageToSpool(index, spool, ColaGetBlkno(0,1,0));

	if (A_ISLINKED(arrToInsert)) {
		ColaSaveOldRLPs(index, arrToInsert, state, spool);
		arrToInsert &= ~CAS_LINKED;
		state->ColaArrayState[A_LEVEL(arrToInsert)][A_ARRNUM(arrToInsert)] = arrToInsert;
		saveColaInsertState(state);
	}

    tuplesort_performsort(spool->sortstate);

	/* Everything is ok. ready to insert */
    while(((itup = tuplesort_getindextuple(spool->sortstate, true, &should_free))!=NULL)&&(inserted)) {
        inserted = ColaInsert(index, state, itup, arrToInsert); 
    }

	_cola_spooldestroy(spool);

    if(!inserted) {
		elog(LOG, "COLA index. _cola_merge_0to1. ColaInsert() failed");
    	return false;
    }
    else {
		/*
		 * All tuples are merged (copied) from level 0 to level 1.
		 * Clear 0 level pages and change states of arrays.
		 */
		/* TODO Find time to change state and clear page. Care about concurrently reading */
		ColaClearPage(index, ColaGetBlkno(0,0,0));
		ColaClearPage(index, ColaGetBlkno(0,1,0));

    	state->ColaArrayState[levelToInsert][arrnumToInsert] |= CAS_FULL | CAS_VISIBLE | CAS_EXISTS;
    	state->ColaArrayState[levelToInsert][arrnumToInsert] &= ~CAS_MERGE;
		state->ColaArrayState[0][0] &= ~CAS_FULL & (~CAS_MERGE);
		state->ColaArrayState[0][1] &= ~CAS_FULL & (~CAS_MERGE);

		/* nOldRLPs was inserted --> array arrToInsert is Linked again */
		if (state->nOldRLPs > 0) {
			state->ColaArrayState[levelToInsert][arrnumToInsert] |= CAS_LINKED;
		}

		saveColaInsertState(state);
    	return true;
    }
}


void
ColaClearPage(Relation index, BlockNumber blkno) {
	int	ndeletable = 0;
	OffsetNumber deletable[MaxOffsetNumber];
	OffsetNumber offnum, maxoff;
	Buffer	buf = ColaGetBuffer(index, blkno);
	Page page = BufferGetPage(buf);

	maxoff = PageGetMaxOffsetNumber(page);

	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
		deletable[ndeletable++] = offnum;

	if (ndeletable > 0) {
		/*
		 * Trade in the initial read lock for a super-exclusive write lock on
		 * this page.  We must get such a lock on every leaf page over the
		 * course of the vacuum scan.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		LockBufferForCleanup(buf);
		PageIndexMultiDelete(page, deletable, ndeletable);
	}
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

/* If there's only 2 arrays at the level and they are merging down,
 * need to complete level (create 3rd array) to keep BlockNumbers order
 */
void
ColaCompleteLevel(Relation index, int level, ColaInsertState *state) {
	int cellMax, i;
	Buffer buf = InvalidBuffer;

	if (!A_ISEXIST(state->ColaArrayState[level][2])) {
		cellMax = (int) pow(2.0, (double)(level));
		for (i = 0; i < cellMax; i++) {
			buf = ColaGetBuffer(index, P_NEW);
			MarkBufferDirty(buf);
			UnlockReleaseBuffer(buf);
		}
	}

	state->ColaArrayState[level][2] |= CAS_EXISTS;
	saveColaInsertState(state);
}


bool LevelIsEmpty(ColaInsertState *state, int level) {
	int count;
	int arrnum;

	for(count = 0, arrnum = 0; arrnum < 3; arrnum++) {
		if ((A_ISVISIBLE(state->ColaArrayState[level][arrnum]))&&(A_ISFULL(state->ColaArrayState[level][arrnum])))
			count++;
	}

	return (count == 0);
}

bool
_cola_merge(Relation index, ColaInsertState *state) {
	bool merged = false;
	int levelFrom, levelTo;
	
	/*start merge from level 1 to level 2*/
	levelFrom = 1;
	levelTo = levelFrom + 1; 

	while ((!LevelIsSafe(levelFrom,state))&&(levelFrom < MaxColaHeight)) {
		state->lastMerge = false;
		
		/* If next level doesn't contain VISIBLE & FULL array, it would be the last merge --> have to save newRLPs */
		if (LevelIsEmpty(state, levelTo)) {

			state->lastMerge = true;
			state->nNewRLPs = (int) pow(2.0, (double)(levelTo));
			state->newRLPs = palloc(sizeof(IndexTuple)*state->nNewRLPs);
		}

		merged = ColaMergeDown(levelFrom, index, state);

		levelFrom++;
		levelTo = levelFrom + 1;
	}

	if ((state->nNewRLPs > 0)) {
		/* Merge is completed at level i->i+1. Now link up level i+1 to i*/
		state->lastMerge = false;
		ColaLinkUp(levelFrom, index, state);
	}

	state->lastMerge = false;
	return merged;
}

int32
_cola_compare(Relation rel, IndexTuple itup1, IndexTuple itup2) {
    bool isNull;
    Datum datum1 = index_getattr(itup1, 1, rel->rd_att, &isNull);
    Datum datum2 = index_getattr(itup2, 1, rel->rd_att, &isNull);
    FmgrInfo *orderFn = index_getprocinfo(rel, 1, COLAORDER_PROC);

    int result = DatumGetInt32(FunctionCall2Coll(orderFn, rel->rd_indcollation[1],
                                                    datum1, datum2));
    return result;
}

void
colaInitMergeFrom (Relation index, MergeArrayInfo* arrInfo, int i, int cell) {
	arrInfo->cell = cell;
	/* cell > 0. It means we have already read cell. Clear it and unlock buffer. */
	if (cell > 0) {
		//Buffer will be locked again in ColaClearPage, so Unlock it before clear
		UnlockReleaseBuffer(arrInfo->buf);
		ColaClearPage(index, arrInfo->blkno);
	}
	arrInfo->blkno = ColaGetBlkno(A_LEVEL(arrInfo->arrState), A_ARRNUM(arrInfo->arrState), arrInfo->cell);
	arrInfo->buf = ColaGetBuffer(index, arrInfo->blkno);
	arrInfo->page = BufferGetPage(arrInfo->buf);
	arrInfo->offnum = FirstOffsetNumber;
}

void
colaMergeRest(Relation index,ColaInsertState *state, MergeArrayInfo* arrInfo, MergeArrayInfo* arrInfoMergeTo, int i, int cellMaxFrom, int cellMaxTo) {
	int compare;

	while (arrInfo->cell < cellMaxFrom) {
		while (arrInfo->offnum <= PageGetMaxOffsetNumber(arrInfo->page)) {

			arrInfo->it = (IndexTuple) PageGetItem(arrInfo->page, PageGetItemId(arrInfo->page, arrInfo->offnum));

			/* Skip RLP if any. */
			while ((arrInfo->it)->t_tid.ip_posid == ColaRLPOffset) {
				arrInfo->offnum = OffsetNumberNext(arrInfo->offnum);

				if (arrInfo->offnum > PageGetMaxOffsetNumber(arrInfo->page))
					break;
				else
					arrInfo->it = (IndexTuple) PageGetItem(arrInfo->page, PageGetItemId(arrInfo->page, arrInfo->offnum));
			}

			if (arrInfo->offnum > PageGetMaxOffsetNumber(arrInfo->page))
				break;

			if (state->curOldRLPs < state->nOldRLPs) {
				compare = _cola_compare(index, arrInfo->it, state->oldRLPs[state->curOldRLPs]);
				// it <= rlp
				if (compare < 1) {
					if (!ColaInsert(index, state, arrInfo->it, arrInfoMergeTo->arrState))
						elog(ERROR, "Debug. colaMergeRest. ColaInsert iti failed");
					arrInfo->offnum = OffsetNumberNext(arrInfo->offnum);
				}
				// rlp < it
				else {
					if (!ColaInsert(index, state, state->oldRLPs[state->curOldRLPs], arrInfoMergeTo->arrState))
						elog(ERROR, "Debug. colaMergeRest. ColaInsert rlp failed");
					state->curOldRLPs++;
				}
			}
			else {
				if (!ColaInsert(index, state, arrInfo->it, arrInfoMergeTo->arrState))
					elog(ERROR, "Debug. colaMergeRest. ColaInsert iti failed");
				arrInfo->offnum = OffsetNumberNext(arrInfo->offnum);
			}
		}
		arrInfo->cell++;

		/* Current cell of arrInfo is ended. Try to get next one. */
		if (arrInfo->cell < cellMaxFrom) {
			colaInitMergeFrom(index, arrInfo, i, arrInfo->cell);
		}
		else {
			UnlockReleaseBuffer(arrInfo->buf);
			ColaClearPage(index, arrInfo->blkno);
		}
	}
}

bool
ColaMergeDown(int levelFrom, Relation index, ColaInsertState *state) {
	int i, levelTo;
	int cellMaxFrom, cellMaxTo;
	uint16 arrFrom;
	MergeArrayInfo *arrInfo1 = (MergeArrayInfo *) palloc(sizeof(MergeArrayInfo));
	MergeArrayInfo *arrInfo2  = (MergeArrayInfo *) palloc(sizeof(MergeArrayInfo));
	MergeArrayInfo *arrInfoMergeTo = (MergeArrayInfo *) palloc(sizeof(MergeArrayInfo));

	/* first of all, complete level if last array at the level is not exists */
	ColaCompleteLevel(index, levelFrom, state);

	/* find array mergeTo */
	levelTo = levelFrom+1;
	cellMaxTo = (int) pow(2.0, (double)(levelTo));
	arrInfoMergeTo->arrState = ColaFindArray(levelTo, state->ColaArrayState);

	if (arrInfoMergeTo->arrState == InvalidColaArrayState) //Shouldn't happent. But concurrency.
		elog(ERROR, "Debug. ColaMergeDown. InvalidColaArrayState arrInfoMergeTo->arrState %d, levelTo %d", arrInfoMergeTo->arrState, levelTo);

	if (A_ISLINKED(arrInfoMergeTo->arrState)) {
		ColaSaveOldRLPs(index, arrInfoMergeTo->arrState, state, NULL);
		arrInfoMergeTo->arrState &= ~CAS_LINKED;
		state->ColaArrayState[levelTo][A_ARRNUM(arrInfoMergeTo->arrState)] = arrInfoMergeTo->arrState;
		saveColaInsertState(state);
	}
	state->currentCell = 0;
	state->curOldRLPs = 0;

	state->needPage = !A_ISEXIST(arrInfoMergeTo->arrState);

	/* find arrnums of arrays mergeFrom */
	arrInfo1->arrState = arrInfo2->arrState = 0;
	for (i = 0; i <= 2; i++) {
		arrFrom = state->ColaArrayState[levelFrom][i];
		if(A_ISVISIBLE(arrFrom)) {
			if(arrInfo1->arrState == 0)
				arrInfo1->arrState = arrFrom;
			else
				arrInfo2->arrState = arrFrom;
		//TODO set state CAS_MERGE for arrFrom here
		}
	}

	if ((arrInfo1->arrState == 0)||(arrInfo2->arrState == 0)||(arrInfo1->arrState == arrInfo2->arrState))
		elog(ERROR, "Debug. ColaMergeDown. Problem with arrFrom");

	//set state CAS_MERGE for arrInfoMergeTo->arrState here and save state at metapage
	arrInfo1->arrState |= CAS_MERGE;
	arrInfo2->arrState |= CAS_MERGE;
	arrInfoMergeTo->arrState |= CAS_MERGE;
	state->ColaArrayState[levelFrom][A_ARRNUM(arrInfo1->arrState)]  = arrInfo1->arrState;
	state->ColaArrayState[levelFrom][A_ARRNUM(arrInfo2->arrState)]  = arrInfo2->arrState;
	state->ColaArrayState[levelTo][A_ARRNUM(arrInfoMergeTo->arrState)]  = arrInfoMergeTo->arrState;
	//elog(NOTICE, "Debug. MergeDown State before merge.\n from1 = %d, from2 = %d, to = %d"
	//						, arrInfo1->arrState, arrInfo2->arrState, arrInfoMergeTo->arrState);
	saveColaInsertState(state);

	cellMaxFrom = (int) pow(2.0, (double)(levelFrom));
	colaInitMergeFrom (index, arrInfo1, 1, 0);
	colaInitMergeFrom (index, arrInfo2, 2, 0);

	/* Ready to start merge */
	while ((arrInfo1->cell < cellMaxFrom)&&(arrInfo2->cell < cellMaxFrom)) {
		//elog(NOTICE, "Debug. ColaMergeDown. arrInfo1->cell = %d arrInfo2->cell = %d", arrInfo1->cell, arrInfo2->cell);
		while((arrInfo1->offnum <= PageGetMaxOffsetNumber(arrInfo1->page))
						&&(arrInfo2->offnum <= PageGetMaxOffsetNumber(arrInfo2->page))) {

			/* Read tuple from arrInfo1. */
			arrInfo1->it = (IndexTuple) PageGetItem(arrInfo1->page, PageGetItemId(arrInfo1->page, arrInfo1->offnum));
			/* Skip RLP if any. */
			while ((arrInfo1->it)->t_tid.ip_posid == ColaRLPOffset) {

				arrInfo1->offnum = OffsetNumberNext(arrInfo1->offnum);
				if (arrInfo1->offnum > PageGetMaxOffsetNumber(arrInfo1->page))
					break;
				else
					arrInfo1->it = (IndexTuple) PageGetItem(arrInfo1->page, PageGetItemId(arrInfo1->page, arrInfo1->offnum));
			}
			if (arrInfo1->offnum > PageGetMaxOffsetNumber(arrInfo1->page))
				break;

			/* Read tuple from arrInfo2. */
			arrInfo2->it = (IndexTuple) PageGetItem(arrInfo2->page, PageGetItemId(arrInfo2->page, arrInfo2->offnum));
			/* Skip RLP if any. */
			while ((arrInfo2->it)->t_tid.ip_posid == ColaRLPOffset) {

				arrInfo2->offnum = OffsetNumberNext(arrInfo2->offnum);
				if (arrInfo2->offnum > PageGetMaxOffsetNumber(arrInfo2->page))
					break;
				else
					arrInfo2->it = (IndexTuple) PageGetItem(arrInfo2->page, PageGetItemId(arrInfo2->page, arrInfo2->offnum));
			}
			if (arrInfo2->offnum > PageGetMaxOffsetNumber(arrInfo2->page))
				break;

			/* Ready to compare two tuples */
			int compare = _cola_compare(index, arrInfo1->it, arrInfo2->it);
			/* it1 <= it2 */
			if (compare < 1) {
				/* Compare it1 with oldRLP if any. Insert lesser of them. */
				if (state->curOldRLPs < state->nOldRLPs) {
					//elog(NOTICE, "Debug. ColaMergeDown. compare it1 & nOldRLPs, state->curOldRLPs %d", state->curOldRLPs);
					compare = _cola_compare(index, arrInfo1->it, state->oldRLPs[state->curOldRLPs]);
					/* it1 <= rlp */
					if (compare < 1) {
						if (!ColaInsert(index, state, arrInfo1->it, arrInfoMergeTo->arrState))
							elog(ERROR, "Debug. ColaMergeDown. ColaInsert it1 failed");
						arrInfo1->offnum = OffsetNumberNext(arrInfo1->offnum);
					}
					/* rlp < it1 */
					else { 
						if (!ColaInsert(index, state, state->oldRLPs[state->curOldRLPs], arrInfoMergeTo->arrState))
							elog(ERROR, "Debug. ColaMergeDown. ColaInsert oldRLP failed");

						state->curOldRLPs++;
					}
				}
				/* Have no OldRLPs, insert tuple. */
				else {
					if (!ColaInsert(index, state, arrInfo1->it, arrInfoMergeTo->arrState))
						elog(ERROR, "Debug. ColaMergeDown. ColaInsert it1 failed");
					arrInfo1->offnum = OffsetNumberNext(arrInfo1->offnum);
				}
			}
			/* it2 < it1 */
			else {
				/* Compare it2 with oldRLP if any. Insert lesser of them. */
				if (state->curOldRLPs < state->nOldRLPs) {
					compare = _cola_compare(index, arrInfo2->it, state->oldRLPs[state->curOldRLPs]);
					/* it2 <= rlp */
					if (compare < 1) {
						if (!ColaInsert(index, state, arrInfo2->it, arrInfoMergeTo->arrState))
							elog(ERROR, "Debug. ColaMergeDown. ColaInsert it2 failed");
						arrInfo2->offnum = OffsetNumberNext(arrInfo2->offnum);
					}
					/* rlp < it2 */
					else { 
						if (!ColaInsert(index, state, state->oldRLPs[state->curOldRLPs], arrInfoMergeTo->arrState))
							elog(ERROR, "Debug. ColaMergeDown. ColaInsert oldRLP failed");
					
						state->curOldRLPs++;
					}
				}
				/* Have no OldRLPs, insert tuple. */
				else {
					if (!ColaInsert(index, state, arrInfo2->it, arrInfoMergeTo->arrState))
						elog(ERROR, "Debug. ColaMergeDown. ColaInsert it2 failed");
					arrInfo2->offnum = OffsetNumberNext(arrInfo2->offnum);
				}
			}
		}
		/* Current cell of arrInfo1 is ended. Try to get next one. */
		if (arrInfo1->offnum > PageGetMaxOffsetNumber(arrInfo1->page)) {

			arrInfo1->cell++;

			if (arrInfo1->cell < cellMaxFrom)
				colaInitMergeFrom(index, arrInfo1, 1, arrInfo1->cell);
			else {
				UnlockReleaseBuffer(arrInfo1->buf);
				ColaClearPage(index, arrInfo1->blkno);
			}
		}
		/* Current cell of arrInfo2 is ended. Try to get next one. */
		if (arrInfo2->offnum > PageGetMaxOffsetNumber(arrInfo2->page))  {

			arrInfo2->cell++;

			if (arrInfo2->cell < cellMaxFrom)
				colaInitMergeFrom(index, arrInfo2, 2, arrInfo2->cell);
			else {
				UnlockReleaseBuffer(arrInfo2->buf);
				ColaClearPage(index, arrInfo2->blkno);
			}
		}
	}

	/* Only one of functions below will be useful */
	colaMergeRest(index, state, arrInfo1, arrInfoMergeTo, 1, cellMaxFrom, cellMaxTo);
	colaMergeRest(index, state, arrInfo2, arrInfoMergeTo, 2, cellMaxFrom, cellMaxTo);

	while (state->curOldRLPs < state->nOldRLPs) {
		if (!ColaInsert(index, state, state->oldRLPs[state->curOldRLPs], arrInfoMergeTo->arrState))
			elog(ERROR, "Debug. ColaMergeDown. ColaMergeRest OldRLPs. ColaInsert oldRLP failed");
		state->curOldRLPs++;
	}

	if (!A_ISEXIST(arrInfoMergeTo->arrState)) {
		state->currentCell++;
		for (i = state->currentCell; state->currentCell < cellMaxTo; state->currentCell++) { 
			arrInfoMergeTo->buf = ColaGetBuffer(index, P_NEW);
			MarkBufferDirty(arrInfoMergeTo->buf);
			UnlockReleaseBuffer(arrInfoMergeTo->buf);
		}
	}

	arrInfo1->arrState &= ~CAS_FULL & (~CAS_MERGE) & (~CAS_VISIBLE);
	arrInfo2->arrState &= ~CAS_FULL & (~CAS_MERGE) & (~CAS_VISIBLE);

	arrInfoMergeTo->arrState |= CAS_FULL | (CAS_VISIBLE) | (CAS_EXISTS);
	arrInfoMergeTo->arrState |= CAS_EXISTS; //TODO It shouldn't be there. Fix correct state change in ColaInsert?
	arrInfoMergeTo->arrState &= ~CAS_MERGE;

	// nOldRLPs was inserted --> array is Linked again
	if (state->nOldRLPs > 0)
		arrInfoMergeTo->arrState |= CAS_LINKED;

	state->ColaArrayState[levelFrom][A_ARRNUM(arrInfo1->arrState)]  = arrInfo1->arrState;
	state->ColaArrayState[levelFrom][A_ARRNUM(arrInfo2->arrState)]  = arrInfo2->arrState;
	state->ColaArrayState[levelTo][A_ARRNUM(arrInfoMergeTo->arrState)]  = arrInfoMergeTo->arrState;

	saveColaInsertState(state);

	state->currentCell = 0; //clear the value

	pfree(arrInfo1);
	pfree(arrInfo2);
	pfree(arrInfoMergeTo);

	return true;
}

// TODO add CAS_LINKED state flags
void
ColaLinkUp(int levelLinkFrom, Relation index, ColaInsertState *state) {
	int levelLinkTo = levelLinkFrom - 1;
	int arrnum;
	uint16 arrLinkTo = InvalidColaArrayState;
	BlockNumber blkno;
	Buffer		buf;
	int cell = 0;
	int i = 0;
	IndexTuple itupRLP;


	for(arrnum = 0; arrnum < 3; arrnum++) {
		if (!A_ISVISIBLE(state->ColaArrayState[levelLinkTo][arrnum]))
			 arrLinkTo = state->ColaArrayState[levelLinkTo][arrnum];
	}

	if (arrLinkTo == InvalidColaArrayState)
		elog(ERROR, "Debug. ColaLinkUp. arrLinkTo %d ", arrLinkTo);

	blkno = ColaGetBlkno(levelLinkTo, A_ARRNUM(arrLinkTo), cell);
	buf = ColaGetBuffer(index, blkno);
	state->lastMerge = false;

	state->nNewRLPs = state->curNewRLPs;

	/* While state->newRLPs is not empty, insert them at levelLinkTo */
	for (i = 0; i < state->nNewRLPs; i++) { //TODO
		itupRLP = state->newRLPs[i];
		if(!ColaPageAddItem(buf, itupRLP, state, levelLinkTo)) {
			cell++;
			UnlockReleaseBuffer(buf);
			blkno = ColaGetBlkno(levelLinkTo, A_ARRNUM(arrLinkTo), cell);
			buf = ColaGetBuffer(index, blkno);
			ColaPageAddItem(buf, itupRLP, state, levelLinkTo);
		}
		pfree(state->newRLPs[i]);
	}
	UnlockReleaseBuffer(buf);

	state->lastMerge = false;
	state->curNewRLPs = state->nNewRLPs = 0;

	pfree(state->newRLPs);

	state->ColaArrayState[levelLinkTo][A_ARRNUM(arrLinkTo)] |= CAS_VISIBLE;
	state->ColaArrayState[levelLinkTo][A_ARRNUM(arrLinkTo)] |= CAS_LINKED;
	saveColaInsertState(state);
}
