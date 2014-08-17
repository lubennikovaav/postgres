--
-- Test Index-only scan plan on GiST indexes
--

CREATE TABLE gist_tbl (b box, p point);

insert into gist_tbl select box(point(0.05*i, 0.05*i), point(0.05*i, 0.05*i)),
			 point(0.05*i, 0.05*i) FROM generate_series(0,100000) as i;


vacuum analyze;

SET enable_seqscan TO false;
SET enable_bitmapscan TO false;
SET enable_indexscan TO false;
SET enable_indexonlyscan TO true;

-- Check singlecolumn index-only scan for point opclass

CREATE INDEX gist_tbl_point_index ON gist_tbl USING gist (p);
EXPLAIN (COSTS OFF)
select p from gist_tbl where p <@ box(point(0,0),  point(100,100)) and length(p::text) < 10;
DROP INDEX gist_tbl_point_index;

-- Check singlecolumn index-only scan for box opclass

CREATE INDEX gist_tbl_box_index ON gist_tbl USING gist (b);
vacuum analyze;
EXPLAIN (COSTS OFF)
select b from gist_tbl where b <@ box(point(5,5),  point(6,6));
DROP INDEX gist_tbl_box_index;

-- Check multicolumn indexonlyscan for gist

CREATE INDEX gist_tbl_multi_index ON gist_tbl USING gist (b, p);
vacuum analyze;
EXPLAIN (COSTS OFF)
select b, p from gist_tbl where ( (b <@ box(point(5,5),  point(6,6))) and (p <@ box(point(5,5),  point(5.5,5.5))));
DROP INDEX gist_tbl_multi_index;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;

DROP TABLE gist_tbl;
