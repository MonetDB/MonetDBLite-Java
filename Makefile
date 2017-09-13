CFLAGS=-DLIBGDK -DLIBMAL -DLIBOPTIMIZER -DLIBSTREAM -DUSE_PTHREAD_LOCKS -DPIC -D_XPG6 -DHAVE_EMBEDDED

INCLUDE_FLAGS= -Isrc/ -Isrc/embeddedjava -Isrc/monetdblite/src/common -Isrc/monetdblite/src/embedded \
-Isrc/monetdblite/src/gdk -Isrc/monetdblite/src/mal/mal -Isrc/monetdblite/src/mal/modules \
-Isrc/monetdblite/src/mal/optimizer -Isrc/monetdblite/src/mal/sqlbackend -Isrc/monetdblite/src/sql/include \
-Isrc/monetdblite/src/sql/common -Isrc/monetdblite/src/sql/server -Isrc/monetdblite/src/sql/storage \
-Isrc/monetdblite/src/sql/storage/bat

ifeq ($(CC),)
    CC = gcc
endif

ifeq ($(OS),) # I am doing cross compilation for MonetDBJavaLite, so I need this
    OS := $(shell uname -s)
endif

ifeq ($(OS),Windows_NT)
    BUILDIR=windows
    SOEXT=dll
    CFLAGS += -m64 -std=c99 -DNATIVE_WIN32
    INCLUDE_FLAGS += -Isrc/embeddedjava/incwindows
    EXTRA_LINK_FLAGS = -lws2_32 -lpthread -lpsapi
    EXTRA_SHARED_FLAGS = -fPIC -Wl,--export-all-symbols
#    ifeq ($(PROCESSOR_ARCHITEW6432),AMD64)
#        CFLAGS += -D AMD64
#    else
#        ifeq ($(PROCESSOR_ARCHITECTURE),AMD64)
#            CFLAGS += -D AMD64
#        endif
#        ifeq ($(PROCESSOR_ARCHITECTURE),x86)
#            CFLAGS += -D IA32
#        endif
#    endif
else ifeq ($(OS),Linux)
    BUILDIR=linux
    SOEXT=so
    CFLAGS += -fPIC
    LDFLAGS = -lm -lpthread -ldl -lrt
    INCLUDE_FLAGS += -Isrc/embeddedjava/inclinux
else ifeq ($(OS),Darwin)
    BUILDIR=macosx
    SOEXT=dylib
    CFLAGS += -fPIC
    LDFLAGS = -lm -lpthread -ldl
    INCLUDE_FLAGS += -Isrc/embeddedjava/incmacosx
else
    $(error The operating system could not be detected)
endif


OPTIMIZE=$(OPT)

ifneq ($(OPTIMIZE), true)
	OPTFLAGS=-g -Wall -Wextra -Werror -Wmissing-prototypes -Wold-style-definition
	OBJDIR=build/$(BUILDIR)/debug
else
	OPTFLAGS=-O3
	OBJDIR=build/$(BUILDIR)/optimized
endif

DEPSDIR=$(OBJDIR)/deps


SQLSCRIPTS=\
src/monetdblite/src/sql/scripts/09_like.sql \
src/monetdblite/src/sql/scripts/10_math.sql \
src/monetdblite/src/sql/scripts/11_times.sql \
src/monetdblite/src/sql/scripts/13_date.sql \
src/monetdblite/src/sql/scripts/15_querylog.sql \
src/monetdblite/src/sql/scripts/16_tracelog.sql \
src/monetdblite/src/sql/scripts/17_temporal.sql \
src/monetdblite/src/sql/scripts/18_index.sql \
src/monetdblite/src/sql/scripts/20_vacuum.sql \
src/monetdblite/src/sql/scripts/21_dependency_functions.sql \
src/monetdblite/src/sql/scripts/22_clients.sql \
src/monetdblite/src/sql/scripts/25_debug.sql \
src/monetdblite/src/sql/scripts/26_sysmon.sql \
src/monetdblite/src/sql/scripts/27_rejects.sql \
src/monetdblite/src/sql/scripts/39_analytics.sql \
src/monetdblite/src/sql/scripts/41_md5sum.sql \
src/monetdblite/src/sql/scripts/51_sys_schema_extension.sql \
src/monetdblite/src/sql/scripts/75_storagemodel.sql \
src/monetdblite/src/sql/scripts/80_statistics.sql \
src/monetdblite/src/sql/scripts/99_system.sql

MALSCRIPTS=\
src/monetdblite/src/mal/modules/aggr.mal \
src/monetdblite/src/mal/modules/algebra.mal \
src/monetdblite/src/mal/modules/bat5.mal \
src/monetdblite/src/mal/modules/batExtensions.mal \
src/monetdblite/src/mal/modules/batmmath.mal \
src/monetdblite/src/mal/modules/batmtime.mal \
src/monetdblite/src/mal/modules/batstr.mal \
src/monetdblite/src/mal/modules/blob.mal \
src/monetdblite/src/mal/modules/group.mal \
src/monetdblite/src/mal/modules/iterator.mal \
src/monetdblite/src/mal/modules/language.mal \
src/monetdblite/src/mal/modules/mal_init.mal \
src/monetdblite/src/mal/modules/manifold.mal \
src/monetdblite/src/mal/modules/mat.mal \
src/monetdblite/src/mal/modules/mkey.mal \
src/monetdblite/src/mal/modules/mmath.mal \
src/monetdblite/src/mal/modules/mtime.mal \
src/monetdblite/src/mal/modules/orderidx.mal \
src/monetdblite/src/mal/modules/pcre.mal \
src/monetdblite/src/mal/modules/sample.mal \
src/monetdblite/src/mal/modules/str.mal \
src/monetdblite/src/mal/modules/tablet.mal \
src/monetdblite/src/mal/optimizer/optimizer.mal \
src/monetdblite/src/mal/sqlbackend/sql.mal \
src/monetdblite/src/mal/sqlbackend/sql_aggr_bte.mal \
src/monetdblite/src/mal/sqlbackend/sql_aggr_dbl.mal \
src/monetdblite/src/mal/sqlbackend/sql_aggr_flt.mal \
src/monetdblite/src/mal/sqlbackend/sql_aggr_int.mal \
src/monetdblite/src/mal/sqlbackend/sql_aggr_lng.mal \
src/monetdblite/src/mal/sqlbackend/sql_aggr_sht.mal \
src/monetdblite/src/mal/sqlbackend/sql_decimal.mal \
src/monetdblite/src/mal/sqlbackend/sql_inspect.mal \
src/monetdblite/src/mal/sqlbackend/sql_rank.mal \
src/monetdblite/src/mal/sqlbackend/sql_transaction.mal \
src/monetdblite/src/mal/sqlbackend/sqlcatalog.mal

MALAUTO=\
src/monetdblite/src/mal/modules/01_batcalc.mal \
src/monetdblite/src/mal/modules/01_calc.mal \
src/monetdblite/src/mal/sqlbackend/40_sql.mal

COBJECTS=\
$(OBJDIR)/monetdblite/src/common/monet_options.o \
$(OBJDIR)/monetdblite/src/common/stream.o \
$(OBJDIR)/monetdblite/src/common/mutils.o \
$(OBJDIR)/monetdblite/src/embedded/embedded.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_aggr.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_align.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_atoms.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_bat.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_batop.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_bbp.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_calc.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_cross.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_delta.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_firstn.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_group.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_hash.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_heap.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_imprints.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_join.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_logger.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_orderidx.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_posix.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_project.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_qsort.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_sample.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_search.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_select.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_ssort.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_storage.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_system.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_tm.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_unique.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_utils.o \
$(OBJDIR)/monetdblite/src/gdk/gdk_value.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_atom.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_builder.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_client.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_dataflow.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_exception.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_function.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_import.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_instruction.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_interpreter.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_linker.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_listing.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_module.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_namespace.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_parser.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_resolve.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_runtime.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_scenario.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_session.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_stack.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_type.o \
$(OBJDIR)/monetdblite/src/mal/mal/mal_utils.o \
$(OBJDIR)/monetdblite/src/mal/modules/aggr.o \
$(OBJDIR)/monetdblite/src/mal/modules/algebra.o \
$(OBJDIR)/monetdblite/src/mal/modules/bat5.o \
$(OBJDIR)/monetdblite/src/mal/modules/batcalc.o \
$(OBJDIR)/monetdblite/src/mal/modules/batExtensions.o \
$(OBJDIR)/monetdblite/src/mal/modules/batmmath.o \
$(OBJDIR)/monetdblite/src/mal/modules/batstr.o \
$(OBJDIR)/monetdblite/src/mal/modules/blob.o \
$(OBJDIR)/monetdblite/src/mal/modules/calc.o \
$(OBJDIR)/monetdblite/src/mal/modules/group.o \
$(OBJDIR)/monetdblite/src/mal/modules/iterator.o \
$(OBJDIR)/monetdblite/src/mal/modules/language.o \
$(OBJDIR)/monetdblite/src/mal/modules/manifold.o \
$(OBJDIR)/monetdblite/src/mal/modules/mat.o \
$(OBJDIR)/monetdblite/src/mal/modules/mkey.o \
$(OBJDIR)/monetdblite/src/mal/modules/mmath.o \
$(OBJDIR)/monetdblite/src/mal/modules/mtime.o \
$(OBJDIR)/monetdblite/src/mal/modules/orderidx.o \
$(OBJDIR)/monetdblite/src/mal/modules/pcre.o \
$(OBJDIR)/monetdblite/src/mal/modules/projectionpath.o \
$(OBJDIR)/monetdblite/src/mal/modules/sample.o \
$(OBJDIR)/monetdblite/src/mal/modules/str.o \
$(OBJDIR)/monetdblite/src/mal/modules/tablet.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_aliases.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_candidates.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_coercion.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_commonTerms.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_constants.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_costModel.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_dataflow.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_deadcode.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_emptybind.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_evaluate.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_garbageCollector.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_generator.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_inline.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_macro.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_matpack.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_mergetable.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_mitosis.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_multiplex.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_pipes.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_prelude.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_profiler.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_projectionpath.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_pushselect.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_remap.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_reorder.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_support.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/opt_wrapper.o \
$(OBJDIR)/monetdblite/src/mal/optimizer/optimizer.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/mal_backend.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/rel_bin.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_assert.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_bat2time.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_cast.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_cat.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_execute.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_fround.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_gencode.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_optimizer.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_orderidx.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_rank.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_result.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_round.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_scenario.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_statement.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_statistics.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_transaction.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_upgrades.o \
$(OBJDIR)/monetdblite/src/mal/sqlbackend/sql_user.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_backend.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_changeset.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_hash.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_keyword.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_list.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_mem.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_stack.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_string.o \
$(OBJDIR)/monetdblite/src/sql/common/sql_types.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_distribute.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_dump.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_exp.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_optimizer.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_partition.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_planner.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_prop.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_psm.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_rel.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_remote.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_schema.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_select.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_semantic.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_sequence.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_trans.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_updates.o \
$(OBJDIR)/monetdblite/src/sql/server/rel_xml.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_atom.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_datetime.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_decimal.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_env.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_mvc.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_parser.tab.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_privileges.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_qc.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_scan.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_semantic.o \
$(OBJDIR)/monetdblite/src/sql/server/sql_symbol.o \
$(OBJDIR)/monetdblite/src/sql/storage/bat/bat_logger.o \
$(OBJDIR)/monetdblite/src/sql/storage/bat/bat_storage.o \
$(OBJDIR)/monetdblite/src/sql/storage/bat/bat_table.o \
$(OBJDIR)/monetdblite/src/sql/storage/bat/bat_utils.o \
$(OBJDIR)/monetdblite/src/sql/storage/bat/nop_logger.o \
$(OBJDIR)/monetdblite/src/sql/storage/bat/res_table.o \
$(OBJDIR)/monetdblite/src/sql/storage/sql_catalog.o \
$(OBJDIR)/monetdblite/src/sql/storage/store.o \
$(OBJDIR)/monetdblite/src/sql/storage/store_dependency.o \
$(OBJDIR)/monetdblite/src/sql/storage/store_sequence.o \
$(OBJDIR)/embeddedjava/converters.o \
$(OBJDIR)/embeddedjava/checknulls.o \
$(OBJDIR)/embeddedjava/embeddedjvm.o \
$(OBJDIR)/embeddedjava/javaids.o \
$(OBJDIR)/embeddedjava/jresultset.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedPreparedStatement.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_jdbc_EmbeddedDataBlockResponse.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_mapping_NullMappings.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_resultset_QueryResultSet.o \
$(OBJDIR)/embeddedjava/nl_cwi_monetdb_embedded_tables_MonetDBTable.o


ODIRS=$(dir $(COBJECTS))
DDIRS=$(subst $(OBJDIR), $(DEPSDIR), $(ODIRS))
$(shell mkdir -p $(ODIRS) $(DDIRS))

LIBFILE=build/$(BUILDIR)/libmonetdb5.$(SOEXT)

.PHONY: all clean test init test $(LIBFILE)

all: $(COBJECTS) $(LIBFILE)


clean:
	rm -rf build


sqlparser: src/monetdblite/src/sql/server/sql_parser.h src/monetdblite/src/sql/server/sql_parser.y
	bison -b src/monetdblite/src/sql/server/sql_parser -y  -d -p sql -r all src/monetdblite/src/sql/server/sql_parser.y
	rm src/monetdblite/src/sql/server/sql_parser.output

inlines: $(MALSCRIPTS) $(SQLSCRIPTS)
	rm -rf build/inlines
	mkdir -p build/inlines/createdb
	mkdir -p build/inlines/autoload
	cp $(MALSCRIPTS)  build/inlines
	cp $(MALAUTO)     build/inlines/autoload
	cp $(SQLSCRIPTS)  build/inlines/createdb
	python src/monetdblite/src/embedded/inlined_scripts.py build/inlines/ ../../src/monetdblite/src/embedded/inlined_scripts.c


init: sqlparser inlines


DEPS = $(shell find $(DEPSDIR) -name "*.d")
-include $(DEPS)


$(OBJDIR)/%.o: src/%.c
	$(CC) $(CFLAGS) -MMD -MF $(subst $(OBJDIR),$(DEPSDIR),$(subst .o,.d,$@)) $(INCLUDE_FLAGS) $(OPTFLAGS) -c $(subst $(OBJDIR)/,src/,$(subst .o,.c,$@)) -o $@

$(LIBFILE): $(COBJECTS)
	$(CC) $(LDFLAGS) $(COBJECTS) $(EXTRA_LINK_FLAGS) $(OPTFLAGS) $(EXTRA_SHARED_FLAGS) -o $(LIBFILE) -shared
