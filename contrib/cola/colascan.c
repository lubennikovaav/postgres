#include "postgres.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "cola.h"

PG_FUNCTION_INFO_V1(colabeginscan);
Datum       colabeginscan(PG_FUNCTION_ARGS);
Datum
colabeginscan(PG_FUNCTION_ARGS)
{
	//elog(NOTICE, "Debug. colabeginscan");
	Relation	rel = (Relation) PG_GETARG_POINTER(0);
	int			nkeys = PG_GETARG_INT32(1);
	int			norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan;
	COLAScanOpaque so;

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (COLAScanOpaque) palloc(sizeof(COLAScanOpaqueData));
	so->indexrel = rel;
	so->colastate = initColaState(rel);
	//TODO allocate in scanCxt?
	memset(so->ColaArrayState, 0, MaxColaHeight*3*sizeof(uint16));
	fillColaScanOpaque(so);

	so->curArrState = so->ColaArrayState[0][0];
	if (!A_ISVISIBLE(so->curArrState)) {
		so->curArrState = InvalidColaArrayState;
		so->curBlkno = 0;
	}
	else
		so->curBlkno = ColaGetBlkno(A_LEVEL(so->curArrState), A_ARRNUM(so->curArrState), 0);

	so->searchFrom = so->curBlkno;
	so->searchTo = so->curBlkno;
	so->rlpFrom = so->rlpTo = 0;
	so->continueArrScan = true;
	so->firstCall = true;
	scan->opaque = so;

	PG_RETURN_POINTER(scan);
}

/*
 * Open next COLA array to scan.
 * TODO: set flag CAS_READ, which means we shouldn't clear chosen array till the end of reading
 * if it was merged down. 
 */
uint16 ColaNextScanArray(COLAScanOpaque so) {
	elog(NOTICE, "ColaNextScanArray level %d arrnum %d", A_LEVEL(so->curArrState), A_ARRNUM(so->curArrState));
	int i;
	int level = A_LEVEL(so->curArrState);
	uint16	newArrState = InvalidColaArrayState;
	int maxArrnum = 3;
	int maxCell;
	if (level == 0)
		maxArrnum = 2;

	
	/* Look for next array at the same level.
	 * Always read arrays using arrnum order.
	 */
	for (i = A_ARRNUM(so->curArrState)+1; i < maxArrnum; i++) {
		//elog(NOTICE, "ColaNextScanArray 1 so->ColaArrayState[%d][%d] %d",level, i, so->ColaArrayState[level][i]);
		if ((A_ISEXIST(so->ColaArrayState[level][i]))&&(!A_ISVISIBLE(so->ColaArrayState[level][i]))&&(A_ISLINKED(so->ColaArrayState[level][i]))) {
			elog(NOTICE, "Magic. ColaNextScanArray %d", so->ColaArrayState[level][i]);
			newArrState = so->ColaArrayState[level][i];
		}
		if ((A_ISVISIBLE(so->ColaArrayState[level][i]))&&(newArrState == InvalidColaArrayState)) {
			//elog(NOTICE, "Chosen ColaNextScanArray  so->ColaArrayState[%d][%d] %d ",level, i, so->ColaArrayState[level][i]);
			newArrState = so->ColaArrayState[level][i];
		}
	}

	/*
	 * Scan COLA, looking for Visible array to read.
	 */
	while ((newArrState == InvalidColaArrayState)&&(level < MaxColaHeight-1)) {
		level++;
		for (i = 0; i < 3; i++) {
			//Is it possible?
			if ((!A_ISEXIST(so->ColaArrayState[level][i]))&&(A_ISVISIBLE(so->ColaArrayState[level][i])))
				elog(ERROR, "COLA array %d doesn't exist, but VISIBLE.", so->ColaArrayState[level][i]);
			if (A_ISVISIBLE(so->ColaArrayState[level][i])) {
				//elog(NOTICE, "ColaNextScanArray 3 so->ColaArrayState[%d][%d] %d ",level, i, so->ColaArrayState[level][i]);
				newArrState = so->ColaArrayState[level][i];
				break;
			}
		}		
	}
	//elog(NOTICE,"A_LEVEL(newArrState) %d, A_LEVEL(so->curArrState) %d",A_LEVEL(newArrState), A_LEVEL(so->curArrState) );
	if ((A_LEVEL(newArrState) <  A_LEVEL(so->curArrState))&&(newArrState != InvalidColaArrayState)) {
		elog(ERROR, "A_LEVEL(newArrState) <  A_LEVEL(so->curArrState)");
	}
	so->curArrState = newArrState;
	so->continueArrScan = true;

	if (so->curArrState  == InvalidColaArrayState) {
		//elog(NOTICE, "Not found visible arrays. It was last filled level.");
	}
	else {
		so->curBlkno = ColaGetBlkno(A_LEVEL(so->curArrState), A_ARRNUM(so->curArrState), 0);
		maxCell = (int) pow(2.0, (double)(A_LEVEL(so->curArrState))) - 1;

		so->maxBlkno = ColaGetBlkno(A_LEVEL(so->curArrState), A_ARRNUM(so->curArrState), maxCell);
		so->searchFrom = so->curBlkno;
		so->searchTo = so->maxBlkno;

		if (so->rlpFrom != 0) {
			//elog(NOTICE, "so->searchFrom %d > so->rlpFrom %d", so->searchFrom, so->rlpFrom);
			so->searchFrom = so->rlpFrom;
			so->rlpFrom = 0;

			if (so->searchFrom < so->curBlkno)
				elog(ERROR, "so->curBlkno %d > so->searchFrom %d", so->curBlkno, so->searchFrom);
			
			so->curBlkno = so->searchFrom;
		}
		
		if (so->rlpTo != 0) {
			so->searchTo = so->rlpTo;
			so->rlpTo = 0;
		}
		
		if (so->searchTo > so->maxBlkno)
			elog(ERROR, "so->searchTo %d > so->maxBlkno %d",so->searchTo, so->maxBlkno);
	}
		//elog (NOTICE, "so->curBlkno %d so->searchTo %d",so->curBlkno, so->searchTo);
	return so->curArrState;
}

PG_FUNCTION_INFO_V1(colarescan);
Datum       colarescan(PG_FUNCTION_ARGS);
Datum
colarescan(PG_FUNCTION_ARGS)
{
	//elog(NOTICE, "Debug. colarescan");
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey     key = (ScanKey) PG_GETARG_POINTER(1);
	COLAScanOpaque so = (COLAScanOpaque) scan->opaque;

	if (so->curArrState != InvalidColaArrayState)
		so->curBlkno = ColaGetBlkno(A_LEVEL(so->curArrState), A_ARRNUM(so->curArrState), 0);
	else
		so->curBlkno = 0;
	memmove(scan->keyData, key, scan->numberOfKeys * sizeof(ScanKeyData));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(colaendscan);
Datum       colaendscan(PG_FUNCTION_ARGS);
Datum
colaendscan(PG_FUNCTION_ARGS)
{
	//elog(NOTICE, "Debug. colaendscan");
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	COLAScanOpaque so = (COLAScanOpaque) scan->opaque;
	pfree(so);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(colagetbitmap);
Datum       colagetbitmap(PG_FUNCTION_ARGS);
Datum
colagetbitmap(PG_FUNCTION_ARGS)
{
	//elog(NOTICE, "Debug. colagetbitmap.");
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap  *tbm = (TIDBitmap *) PG_GETARG_POINTER(1);
	COLAScanOpaque so = (COLAScanOpaque) scan->opaque;
	int64		ntids = 0;

	so->curPageData = so->nPageData = 0;

	/*
	 * While scanning a leaf page, ItemPointers of matching heap tuples will
	 * be stored directly into tbm, so we don't need to deal with them here.
	 */

	while(so->curArrState != InvalidColaArrayState) {
		while ((so->curBlkno <= so->searchTo)&&(so->continueArrScan)) {
			colaScanPage(scan, so->curBlkno, tbm, &ntids);
			so->curBlkno++;
		}
		ColaNextScanArray(so);
	}

	PG_RETURN_INT64(ntids);
}

/*	 <0 if scankey < tuple;
 *   0 if scankey == tuple;
 *   >0 if scankey > tuple.
 */
//TODO Require rewrite for multicolumn key
//TODO check bugfix: orderFn instead of sk_func
int32
_cola_compare_keys(IndexScanDesc scan, IndexTuple tuple)
{
	TupleDesc	tupdesc;
	int			keysz;
	int			ikey;
	ScanKey		key;
	int			result;
	Relation rel = scan->indexRelation;
	tupdesc = RelationGetDescr(scan->indexRelation);
	keysz = scan->numberOfKeys;
	for (key = scan->keyData, ikey = 0; ikey < keysz; key++, ikey++)
	{
		Datum		datum;
		bool		isNull = false;
		datum = index_getattr(tuple,
							  key->sk_attno,
							  tupdesc,
							  &isNull);

	   	FmgrInfo *orderFn = index_getprocinfo(rel, 1, COLAORDER_PROC);

	   	result = DatumGetInt32(FunctionCall2Coll(orderFn, key->sk_collation,
		                                                 datum, key->sk_argument));

	//elog(NOTICE, "Debug._cola_compare_keys. itup %d  key %d result = %d", DatumGetInt32(datum), DatumGetInt32(key->sk_argument), result?1:0 );
	}
	//elog(NOTICE, "Debug. _cola_compare_keys result %d", result);
	return result;
}

// TODO fix error
bool
_cola_checkkeys(IndexScanDesc scan,COLAScanOpaque so, IndexTuple tuple)
{
	TupleDesc	tupdesc;
	int			keysz;
	int			ikey;
	ScanKey		key;

	tupdesc = RelationGetDescr(scan->indexRelation);
	keysz = scan->numberOfKeys;
	for (key = scan->keyData, ikey = 0; ikey < keysz; key++, ikey++)
	{
		Datum		datum;
		bool		isNull = false;
		Datum		test;
		datum = index_getattr(tuple,
							  key->sk_attno,
							  tupdesc,
							  &isNull);

		//elog(NOTICE, "_cola_checkkeys tuple %d", DatumGetInt32(datum));

		test = FunctionCall2Coll(&key->sk_func, key->sk_collation,
								 datum, key->sk_argument);
		if (!DatumGetBool(test))
		{
			/*
			 * This indextuple doesn't match the qual.
			 */
			if ((key->sk_strategy == BTLessStrategyNumber)||(key->sk_strategy == BTLessEqualStrategyNumber)) {
				/* itup > key. All tuples in the array are sorted, so this array doesn't contain any more tuples matches the key */
				so->continueArrScan = false;
			}
			return false;
		}
	}
	/* If we get here, the tuple passes all index quals. */
	//elog(NOTICE, "Debug. _cola_checkkeys true");
	return true;
}

bool
ColaFindRlp(IndexScanDesc scan, COLAScanOpaque so, IndexTuple it) {
	ScanKey key = scan->keyData;
	if (IndexTupleIsRLP(it)) {

		if ((key->sk_strategy == BTLessStrategyNumber)||(key->sk_strategy == BTLessEqualStrategyNumber)) {
			if (!_cola_checkkeys(scan, so, it)) {
				so->rlpTo = BlockIdGetBlockNumber(&it->t_tid.ip_blkid);
			}
		}
		else if ((key->sk_strategy == BTGreaterStrategyNumber)||(key->sk_strategy == BTGreaterEqualStrategyNumber)) {
			if (!_cola_checkkeys(scan, so, it)) {
				so->rlpFrom = BlockIdGetBlockNumber(&it->t_tid.ip_blkid);
			}
		}
		else { //CompareStrategy Equal
			if (_cola_compare_keys(scan, it) > 0) {
				so->rlpTo = BlockIdGetBlockNumber(&it->t_tid.ip_blkid);
			}
			else if (_cola_compare_keys(scan, it) < 0) {
				so->rlpFrom = BlockIdGetBlockNumber(&it->t_tid.ip_blkid);
			}
			else {
				//do nothing
				// flag look_around: If index is not unique, next level cound contain multiple tuples with the same key.
				// And some of them could be moreleft than rlpFrom block or moreright that rlpTo block.
				//
			} 
		}
			
		return true;
	}
	return false;
}

void
colaScanPage(IndexScanDesc scan, BlockNumber blkno, TIDBitmap *tbm, int64 *ntids)
{
	//elog(NOTICE, "Debug. colaScanPage. blkno %d", blkno);
	COLAScanOpaque so = (COLAScanOpaque) scan->opaque;
	Buffer		buffer;
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber i;

	buffer = ReadBuffer(scan->indexRelation, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);

	so->nPageData = so->curPageData = 0;

	/*
	 * check all tuples on page
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		IndexTuple	it = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
		bool match;

		if (A_ISLINKED(so->curArrState)) {
			if (ColaFindRlp(scan, so, it))
				continue;
		}

		match = _cola_checkkeys(scan, so, it);

		if ((!match)&&(!so->continueArrScan)) {
			/*
			 * If array is linked & rlpTo==0 -- continue array scan to find rlp
			 * TODO: if BTGreaterStrategyNumber/BTGreaterEqualStrategyNumber --> rlpTo will always be 0.
			 * Add flag need rlpTo? No. I'm too lazy.
			 * If A_ISLINKED && !A_ISFULL --> it contains RLPs only, so it's not too hard to read all.
			 * Otherwise, stop reading right now, don't care about rlpTo
			*/
			if (A_ISLINKED(so->curArrState)&&(!A_ISFULL(so->curArrState))&&(so->rlpTo == 0)) {
				so->continueArrScan = true;
			}
			/* If current level == 0, continue scan anyway, because tuples at level 0 are unsorted*/
			if (A_LEVEL(so->curArrState) == 0) {
				so->continueArrScan = true;
			}
			/* If reason to continue scan is not founded, break page scan and go to next cola array.*/
			if(!so->continueArrScan)
				break;
		}

		if (!match)
			continue; /* This indextuple doesn't match the qual */

		/* If we get here, the tuple passes all index quals. Retun indexTuple */

		if (tbm)
		{
			/*
			 * getbitmap scan, so just push heap tuple TIDs into the bitmap
			 * without worrying about ordering
			 */
			tbm_add_tuples(tbm, &it->t_tid, 1, false);
			(*ntids)++;
			//elog(NOTICE, "Debug. colaScanPage, tbm_add_tuples. ntids = %ld", *ntids);
		}

		if (scan->numberOfOrderBys == 0)
		{
			/*
			 * Non-ordered scan, so report heap tuples in so->pageData[]
			 */
			so->pageData[so->nPageData].heapPtr = it->t_tid;
			so->nPageData++;
		}
		else
			elog(ERROR, "COLA doesn't support ordered scan");
	}

	UnlockReleaseBuffer(buffer);
}

PG_FUNCTION_INFO_V1(colagettuple);
Datum       colagettuple(PG_FUNCTION_ARGS);
Datum
colagettuple(PG_FUNCTION_ARGS)
{
	//elog(NOTICE, "Debug. colagettuple");
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanDirection dir = (ScanDirection) PG_GETARG_INT32(1);
	COLAScanOpaque so = (COLAScanOpaque) scan->opaque;

	if (dir != ForwardScanDirection)
		elog(ERROR, "COLA only supports forward scan direction");

	if (so->firstCall)
	{
		pgstat_count_index_scan(scan->indexRelation);
		so->firstCall = false;
		so->curPageData = so->nPageData = 0;
		
		so->curArrState = so->ColaArrayState[0][0];
		if (!A_ISVISIBLE(so->curArrState)) {
			so->curArrState = InvalidColaArrayState;
			so->curBlkno = 0;
		}
		else
			so->curBlkno = ColaGetBlkno(A_LEVEL(so->curArrState), A_ARRNUM(so->curArrState), 0);
	}

	for (;;)
	{
		if (so->curPageData < so->nPageData) {
			/* continuing to return tuples from a leaf page */
			scan->xs_ctup.t_self = so->pageData[so->curPageData].heapPtr;
			so->curPageData++;
			PG_RETURN_BOOL(true);
		}
		else {
			if(so->curArrState != InvalidColaArrayState) {
				if ((so->curBlkno <= so->searchTo)&&(so->continueArrScan)) {
					colaScanPage(scan, so->curBlkno, NULL, NULL);
					so->curBlkno++;
				}
				else
					ColaNextScanArray(so);
			}
			if 	((so->curArrState == InvalidColaArrayState)&&(so->curPageData >= so->nPageData))
				PG_RETURN_BOOL(false);
		}
	}
}

PG_FUNCTION_INFO_V1(colamarkpos);
Datum       colamarkpos(PG_FUNCTION_ARGS);
Datum
colamarkpos(PG_FUNCTION_ARGS)
{
    elog(ERROR, "COLA does not support mark/restore. From colamarkpos().");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(colarestrpos);
Datum       colarestrpos(PG_FUNCTION_ARGS);
Datum
colarestrpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "COLA does not support mark/restore. From colarestpos().");
	PG_RETURN_VOID();
}
