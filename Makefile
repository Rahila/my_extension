MODULES = my_extension

EXTENSION = my_extension
DATA = my_extension--1.0.sql
PGFILEDESC = "Write extension demo"

REGRESS = my_extension_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
