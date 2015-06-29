CREATE OR REPLACE FUNCTION colabuild(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colabuildempty(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colainsert(internal, internal, internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colaoptions(text[], bool)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colabeginscan(internal, int4, int4)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colarescan(internal, internal, int4, internal, int4)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colaendscan(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colamarkpos(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colarestrpos(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colagetbitmap(internal, internal)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colabulkdelete(internal, internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colavacuumcleanup(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION colacostestimate(internal, internal, float8, internal, internal, internal, internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

--access method
CREATE ACCESS METHOD cola (
	nstrategies = 5,
	nsupport = 1,
	canorder = false,
	canorderbyop = false,
	canbackward = false,
	canunique = false,
	canmulticol = true,
	optionalkey = true,
	searcharray = false,
	searchnulls = false,
	storage = false,
	clusterable = false,
	predlocks = false,
	keytype = internal,
	insert = colainsert,
	beginscan = colabeginscan,
	getbitmap = colagetbitmap,
	rescan = colarescan,
	getbitmap = colagetbitmap,
	endscan = colaendscan,
	markpos = colamarkpos,
	restrpos = colarestrpos,
	build = colabuild,
	buildempty = colabuildempty,
	bulkdelete = colabulkdelete,
	vacuumcleanup = colavacuumcleanup,
	costestimate = colacostestimate,
	options = colaoptions
);

--opclasses

CREATE OPERATOR CLASS int4_ops 
DEFAULT FOR TYPE int4 USING cola AS
	OPERATOR	1	<(int4, int4),
	OPERATOR	2	<=(int4, int4),
	OPERATOR	3	=(int4, int4),
	OPERATOR	4	>=(int4, int4),
	OPERATOR	5	>(int4, int4),
	FUNCTION	1	btint4cmp(int4,int4);
/* maxOpNumber set via nstrategies = 3,
 * maxProcNumber set via nsupport = 1 (see above)
 */
