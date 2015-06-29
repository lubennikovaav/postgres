# contrib/cola/Makefile

MODULE_big = cola
OBJS = colainsert.o colautils.o colascan.o colavacuum.o colacost.o colamergeutils.o colasort.o $(WIN32RES)

EXTENSION = cola
DATA = cola--1.0.sql
PGFILEDESC = "cola access method - based on write-optimised data structure"

REGRESS = cola

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cola
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
