#ifndef COLA_H
#define COLA_H

#include "access/genam.h"
#include "access/itup.h"
#include "fmgr.h"
#include "utils/tuplesort.h"

#define MaxColaHeight		20 /* Change number of MaxLevels. */
#define ColaRLPOffset		0 /* RLP Offset. Should be Invalid or never used OffsetNumber */
#define	SizeOfRLP			32 /* TODO: this size should be computed while inserting. Or by known size of datatype */

typedef struct ColaPageOpaqueData
{
	uint16			flags;
} ColaPageOpaqueData;

typedef ColaPageOpaqueData *ColaPageOpaque;

/*
 * ColaMetaPageData
 */
typedef struct ColaMetaPageData
{
	uint32		cola_magic;		/* should contain COLA_MAGIC */
	uint16 ColaArrayState[MaxColaHeight][3];
} ColaMetaPageData;	


/* Bits defined ColaArrayState flags */
#define CAS_MERGE        (1 << 0)    /* array is in merge now */
#define CAS_LINKED       (1 << 1)    /* array is linked to lower level */
#define CAS_FULL         (1 << 2)    /* array is full */
#define CAS_VISIBLE      (1 << 3)    /* array is visible to read */
#define CAS_EXISTS       (1 << 4)    /* array is exists */

#define InvalidColaArrayState	1 /* 9 corresponds to CAS_MERGE, !CAS_EXISTS array 0 at level 0. This state is impossible. */

/* Macros to get cola array state */
#define A_ISMERGE(cas)        (cas & CAS_MERGE)
#define A_ISLINKED(cas)       (cas & CAS_LINKED)
#define A_ISFULL(cas)         (cas & CAS_FULL)
#define A_ISVISIBLE(cas)      (cas & CAS_VISIBLE)
#define A_ISEXIST(cas)        (cas & CAS_EXISTS)
#define A_LEVEL(cas)          ((cas >> 7) & 31)
#define A_ARRNUM(cas)         ((cas >> 5) & 3)


#define COLA_MAGIC	0x011BED
#define COLA_METAPAGE	0

#define COLA_META		(1<<0)
#define ColaPageGetOpaque(page) ( (ColaPageOpaque) PageGetSpecialPointer(page) )
#define ColaPageIsMeta(page) ( ColaPageGetOpaque(page)->flags & COLA_META)

#define ColaPageGetData(page)		(  (IndexTuple*)PageGetContents(page) )

#define ColaPageGetMeta(p) \
	((ColaMetaPageData *) PageGetContents(p))

/*
 * TODO copied from BTORDER_PROC
 *  When a new operator class is declared, we require that the user
 *  supply us with an amproc procedure (COLAORDER_PROC) for determining
 *  whether, for two keys a and b, a < b, a = b, or a > b.  This routine
 *  must return < 0, 0, > 0, respectively, in these three cases.
 */

#define COLAORDER_PROC        1

#define IndexTupleIsRLP(itup) \
( \
	(itup->t_tid.ip_posid) == ColaRLPOffset  \
)

/*
 * ColaState TODO
 */
typedef struct ColaState 
{
	MemoryContext 	scanCxt;		/* context for scan-lifespan data */
	MemoryContext 	tempCxt;		/* short-term context for calling functions */
	TupleDesc 	tupdesc;			/* index's tuple descriptor */
	FmgrInfo	orderFn[INDEX_MAX_KEYS]; /* COLAORDER_PROC */
} ColaState;

typedef struct MergeArrayInfo {
	int 			cell;
	BlockNumber 	blkno;
	Buffer 			buf;
	Page 			page;
	OffsetNumber 	offnum;
	IndexTuple		it;
	uint16			arrState;
} MergeArrayInfo;

typedef struct ColaInsertState
{
	ColaState		*colastate;
	Relation 		indexrel;
	Buffer			currentBuffer;
	int 			currentCell;
	bool 			needPage;
	bool			lastMerge;

	uint16 ColaArrayState[MaxColaHeight][3];

	IndexTuple		*newRLPs; //array of RLPs saved while MergeDown
	int				nNewRLPs; //count of items in newRLPs
	int				curNewRLPs; //current last item in newRLPs

	IndexTuple		*oldRLPs; //array of RLPs that we found in array where inserting //TODO rewrite
	int				nOldRLPs; //count of items in oldRLPs
	int				curOldRLPs; //current last item in oldRLPs
} ColaInsertState;

typedef struct ColaSpool
{
	Tuplesortstate *sortstate;	// state data for tuplesort.c 
	Relation	index;
	TupleDesc 	tupdesc;			/* index's tuple descriptor */
} ColaSpool;


typedef struct COLASearchHeapItem
{
	ItemPointerData 	heapPtr;	/* TID of referenced heap item */
	IndexTuple 	ftup;

} COLASearchHeapItem;

typedef struct COLAScanOpaqueData
{
	COLASearchHeapItem 	pageData [BLCKSZ/sizeof(IndexTupleData)];
	Relation 			indexrel;
	uint16				curArrState;
	BlockNumber			curBlkno;
	BlockNumber			maxBlkno;
	OffsetNumber		curPageData;
	OffsetNumber		nPageData;

	ColaState		*colastate;
	uint16			ColaArrayState[MaxColaHeight][3];
	bool			continueArrScan;
	bool			firstCall;

	/* Save rlp while search at level i. When search at level i is finished, copy this values (if !=0 ) to searchFrom and searchTo*/
	/* If level i contains RLP links to level i+1, save here borders of search at level i+1. */
	BlockNumber			rlpFrom;
	BlockNumber			rlpTo;
	/* Borders of search at level i. */
	BlockNumber			searchFrom;
	BlockNumber			searchTo;

} COLAScanOpaqueData;

typedef COLAScanOpaqueData *COLAScanOpaque;

/* colascan.c */
extern void colaScanPage(IndexScanDesc scan, BlockNumber blkno, TIDBitmap *tbm, int64 *ntids);
extern bool _cola_checkkeys(IndexScanDesc scan,COLAScanOpaque so, IndexTuple tuple);
extern uint16 ColaNextScanArray(COLAScanOpaque so);
extern int32 _cola_compare_keys(IndexScanDesc scan, IndexTuple tuple);
extern bool ColaFindRlp(IndexScanDesc scan, COLAScanOpaque so, IndexTuple it);

/* colainsert.c */
extern bool ColaTryInsert(Relation index, ColaInsertState *insstate, IndexTuple itup);
extern bool _cola_doinsert(Relation index, ColaInsertState *insstate, IndexTuple itup);

/* colautils.c */
extern ColaState* initColaState(Relation index);
extern void freeColaState(ColaState *colastate);
extern MemoryContext createTempColaContext(void);
extern ColaInsertState* initColaInsertState(Relation index);
extern void fillColaInsertState(ColaInsertState *state);
extern void fillColaScanOpaque(COLAScanOpaque so);
extern ColaInsertState* saveColaInsertState(ColaInsertState *state);
extern void ColaInitPage(Page page, Size size);
extern void ColaSaveRLP(Buffer buf, IndexTuple itup, ColaInsertState *state);
extern bool ColaPageAddItem(Buffer buffer, IndexTuple itup, ColaInsertState *insstate, int levelToInsert);
extern void ColaInitMetaBuffer(Buffer b);
extern Buffer ColaGetBuffer(Relation rel, BlockNumber blkno);
extern BlockNumber ColaGetBlkno(int level, int array_num, int cell_num);
extern uint16 ColaFindArray(int level, uint16 cas[MaxColaHeight][3]);
extern bool LevelIsSafe(int level, ColaInsertState *state);

/* colamergeutils.c */
extern void ColaAddPageToSpool(Relation index, ColaSpool *spool, BlockNumber blkno);
extern bool ColaInsert(Relation index, ColaInsertState *insstate, IndexTuple itup, int arrToInsert);
extern void ColaSaveOldRLPs(Relation index, uint16 arr, ColaInsertState *state, ColaSpool *spool);
extern bool _cola_merge_0to1(Relation index, ColaInsertState *state);
extern void ColaClearPage(Relation rel, BlockNumber blkno);
extern void ColaCompleteLevel(Relation index, int level, ColaInsertState *state);
extern bool LevelIsEmpty(ColaInsertState *state, int level);
extern bool _cola_merge(Relation index, ColaInsertState *state);
extern int32 _cola_compare(Relation rel, IndexTuple itup1, IndexTuple itup2);
extern void colaInitMergeFrom (Relation index, MergeArrayInfo* arrInfo, int i, int cell);
extern void colaMergeRest(Relation index,ColaInsertState *state,
			MergeArrayInfo* arrInfo, MergeArrayInfo* arrInfoMergeTo, int i, int cellMaxFrom, int cellMaxTo);
extern bool ColaMergeDown(int levelFrom, Relation index, ColaInsertState *state);
extern void ColaLinkUp(int levelLinkFrom,int arrnumLinkTo, Relation index, ColaInsertState *state);

/* colasort.c */
extern ColaSpool* _cola_spoolinit(Relation index, TupleDesc tupdesc);
extern void _cola_spooldestroy(ColaSpool *colaspool);
extern void _cola_spool_tuple(ColaSpool *colaspool, IndexTuple itup);

#endif
