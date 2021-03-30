EXTENSION = kadb_fdw
MODULES = kadb_fdw

EXTENSION_VERSION = 0.10.2
EXTENSION_TAG = $(shell git describe --tags --abbrev=0)

DATA = \
	kadb_fdw--0.5.sql \
	kadb_fdw--0.5--0.6.sql \
	kadb_fdw--0.6--0.7.sql \
	kadb_fdw--0.7--0.8.sql \
	kadb_fdw--0.8--0.9.sql \
	kadb_fdw--0.9--0.10.sql \
	kadb_fdw--0.10--0.10.1.sql \
	kadb_fdw--0.10.1--0.10.2.sql

DATA_built = kadb_fdw--$(EXTENSION_VERSION).sql


OBJS = \
src/execution.o \
src/kafka_consumer.o \
src/kafka_functions.o \
src/offsets.o \
src/planning.o \
src/settings.o \
src/deserialization/api.o \
src/deserialization/attribute_postgres.o \
src/deserialization/avro_deserializer.o \
src/deserialization/csv_deserializer.o \
src/deserialization/format.o \
src/deserialization/text_deserializer.o \
src/functions/auxiliary.o \
src/functions/extra.o \
src/utils/kadb_gp_utils.o \
src/kadb_fdw.o

MODULE_big = kadb_fdw


PG_CFLAGS += -I$(CURDIR)/src
PG_CFLAGS += -Wformat -Wall -Wextra -Wno-unused-parameter

SHLIB_LINK += -lrdkafka -lavro -lcsv -lgmp


REGRESS = update partition_distribution options cursors two_cursors cursors_extra csv miscellaneous text


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


tag_version_check:
ifneq ($(EXTENSION_TAG), $(EXTENSION_VERSION))
	$(error Build tag '$(EXTENSION_TAG)' does not match the extension version '$(EXTENSION_VERSION)')
endif

kadb_fdw--$(EXTENSION_VERSION).sql: $(DATA) tag_version_check
	cat $(DATA) > kadb_fdw--$(EXTENSION_VERSION).sql


# Kafka instance setup: recurse to a dedicated Makefile
kafka-setup: kafka-clean
	$(MAKE) -C kafka_test

# Kafka instance cleanup: recurse to a dedicated Makefile
kafka-clean:
	$(MAKE) clean -C kafka_test

# We want to run normal 'installcheck', but with a different schedule
# And we also want to execute 'kafka-setup' BEFORE installcheck
# This is the most straightforward way to accomplish these two tasks
ifeq ($(MAKECMDGOALS), installcheck-with-kafka)
REGRESS += kafka kafka_avro kafka_offsets kafka_offsets_functions
installcheck: kafka-setup
endif

installcheck-with-kafka: installcheck

# Here, the order does not matter
clean-with-kafka: kafka-clean clean
