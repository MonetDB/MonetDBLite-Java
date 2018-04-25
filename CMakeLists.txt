# CMakeList.txt : CMake project for MonetDLiteJava, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS 1)

add_definitions(-DLIBGDK -DLIBMAL -DLIBOPTIMIZER -DLIBSTREAM -DHAVE_EMBEDDED -DHAVE_EMBEDDED_JAVA)

if (WIN32)
    set(CMAKE_CXX_FLAGS_DEBUG "-D_CRT_SECURE_NO_WARNINGS -DMONETDBLITE_COMPILE")
    set(CMAKE_CXX_FLAGS_RELEASE "-D_CRT_SECURE_NO_WARNINGS -DMONETDBLITE_COMPILE")
endif()

if (UNIX OR APPLE)
    IF(DEFINED ENV{OPT})
        set(CMAKE_C_FLAGS "-O3 -fPIC -Wfatal-errors -Wall -DNDEBUG")
    ELSE()
        set(CMAKE_C_FLAGS "-g -O0 -fPIC -Wfatal-errors -Wall -Wextra -Werror -Wmissing-prototypes -Wold-style-definition")
    ENDIF()
endif()

if(NOT(WIN32 OR UNIX OR APPLE))
    message( FATAL_ERROR "MonetDBLite is only available to Windows, Linux and MacOS X" )
endif()

add_custom_target(sqlparser bison -b ${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/server/sql_parser -y  -d -p sql -r all ${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/server/sql_parser.y
        COMMAND rm ${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/server/sql_parser.output)

set(SQLSCRIPTS                 "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/09_like.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/10_math.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/11_times.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/13_date.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/15_querylog.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/16_tracelog.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/17_temporal.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/18_index.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/20_vacuum.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/21_dependency_functions.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/22_clients.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/25_debug.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/26_sysmon.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/27_rejects.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/39_analytics.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/41_md5sum.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/51_sys_schema_extension.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/75_storagemodel.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/80_statistics.sql")
set(SQLSCRIPTS "${SQLSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/sql/scripts/99_system.sql")

set(MALSCRIPTS                 "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/aggr.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/algebra.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/bat5.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/batExtensions.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/batmmath.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/batmtime.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/batstr.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/blob.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/group.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/iterator.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/language.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/mal_init.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/manifold.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/mat.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/mkey.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/mmath.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/mtime.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/orderidx.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/pcre.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/sample.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/str.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/tablet.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/optimizer/optimizer.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_aggr_bte.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_aggr_dbl.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_aggr_flt.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_aggr_int.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_aggr_lng.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_aggr_sht.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_decimal.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_inspect.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_rank.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sql_transaction.mal")
set(MALSCRIPTS "${MALSCRIPTS}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/sqlcatalog.mal")

set(MALAUTO              "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/01_batcalc.mal")
set(MALAUTO "${MALAUTO}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/modules/01_calc.mal")
set(MALAUTO "${MALAUTO}" "${CMAKE_SOURCE_DIR}/src/monetdblite/src/mal/sqlbackend/40_sql.mal")

add_custom_target(inlines rm -rf build/inlines
        COMMAND mkdir -p build/inlines/createdb
        COMMAND mkdir -p build/inlines/autoload
        COMMAND cp ${MALSCRIPTS}  build/inlines
        COMMAND cp ${MALAUTO}     build/inlines/autoload
        COMMAND cp ${SQLSCRIPTS}  build/inlines/createdb
        COMMAND python ${CMAKE_SOURCE_DIR}/src/monetdblite/src/embedded/inlined_scripts.py build/inlines/ ${CMAKE_SOURCE_DIR}/src/monetdblite/src/embedded/inlined_scripts.c)

include_directories(
        src/monetdblite/src
        src/monetdblite/src/common
        src/monetdblite/src/gdk
        src/monetdblite/src/mal/mal
        src/monetdblite/src/mal/modules
        src/monetdblite/src/mal/optimizer
        src/monetdblite/src/mal/sqlbackend
        src/monetdblite/src/sql/include
        src/monetdblite/src/sql/common
        src/monetdblite/src/sql/server
        src/monetdblite/src/sql/storage
        src/monetdblite/src/sql/storage/bat
        src/monetdblite/src/embedded
        src/embeddedjava
)

if (WIN32)
    set(INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} src/monetdblite/src/windows)
endif()

add_library(monetdb5 SHARED
        src/monetdblite/src/monetdb_config.h
        src/monetdblite/src/windows/dirent.h
        src/monetdblite/src/embedded/undef.h
        src/monetdblite/src/common/mutils.h
        src/monetdblite/src/common/stream.h
        src/monetdblite/src/gdk/gdk.h
        src/monetdblite/src/gdk/gdk_posix.h
        src/monetdblite/src/gdk/gdk_atoms.h
        src/monetdblite/src/gdk/gdk_utils.h
        src/monetdblite/src/gdk/gdk_system.h
        src/monetdblite/src/gdk/gdk_calc_compare.h
        src/monetdblite/src/gdk/gdk_private.h
        src/monetdblite/src/gdk/gdk_system_private.h
        src/monetdblite/src/gdk/gdk_calc_private.h
        src/monetdblite/src/gdk/gdk_bbp.h
        src/monetdblite/src/gdk/gdk_calc.h
        src/monetdblite/src/gdk/gdk_hash.h
        src/monetdblite/src/gdk/gdk_cand.h
        src/monetdblite/src/gdk/gdk_qsort_impl.h
        src/monetdblite/src/gdk/gdk_ssort_impl.h
        src/monetdblite/src/gdk/gdk_delta.h
        src/monetdblite/src/gdk/gdk_atomic.h
        src/monetdblite/src/gdk/gdk_tm.h
        src/monetdblite/src/gdk/gdk_imprints.h
        src/monetdblite/src/gdk/gdk_storage.h
        src/monetdblite/src/gdk/gdk_logger.h
        src/monetdblite/src/mal/mal/mal.h
        src/monetdblite/src/mal/mal/mal_dataflow.h
        src/monetdblite/src/mal/mal/mal_interpreter.h
        src/monetdblite/src/mal/mal/mal_builder.h
        src/monetdblite/src/mal/mal/mal_import.h
        src/monetdblite/src/mal/mal/mal_stack.h
        src/monetdblite/src/mal/mal/mal_resolve.h
        src/monetdblite/src/mal/mal/mal_runtime.h
        src/monetdblite/src/mal/mal/mal_function.h
        src/monetdblite/src/mal/mal/mal_private.h
        src/monetdblite/src/mal/mal/mal_scenario.h
        src/monetdblite/src/mal/mal/mal_linker.h
        src/monetdblite/src/mal/mal/mal_listing.h
        src/monetdblite/src/mal/mal/mal_errors.h
        src/monetdblite/src/mal/mal/mal_type.h
        src/monetdblite/src/mal/mal/mal_exception.h
        src/monetdblite/src/mal/mal/mal_namespace.h
        src/monetdblite/src/mal/mal/mal_client.h
        src/monetdblite/src/mal/mal/mal_atom.h
        src/monetdblite/src/mal/mal/mal_session.h
        src/monetdblite/src/mal/mal/mal_module.h
        src/monetdblite/src/mal/mal/mal_parser.h
        src/monetdblite/src/mal/mal/mal_instruction.h
        src/monetdblite/src/mal/mal/mal_utils.h
        src/monetdblite/src/mal/modules/mmath.h
        src/monetdblite/src/mal/modules/tablet.h
        src/monetdblite/src/mal/modules/batmmath.h
        src/monetdblite/src/mal/modules/bat5.h
        src/monetdblite/src/mal/modules/group.h
        src/monetdblite/src/mal/modules/manifold.h
        src/monetdblite/src/mal/modules/batExtensions.h
        src/monetdblite/src/mal/modules/mat.h
        src/monetdblite/src/mal/modules/sample.h
        src/monetdblite/src/mal/modules/blob.h
        src/monetdblite/src/mal/modules/mtime.h
        src/monetdblite/src/mal/modules/orderidx.h
        src/monetdblite/src/mal/modules/language.h
        src/monetdblite/src/mal/modules/algebra.h
        src/monetdblite/src/mal/modules/str.h
        src/monetdblite/src/mal/modules/projectionpath.h
        src/monetdblite/src/mal/modules/iterator.h
        src/monetdblite/src/mal/modules/mkey.h
        src/monetdblite/src/mal/optimizer/opt_mitosis.h
        src/monetdblite/src/mal/optimizer/opt_garbageCollector.h
        src/monetdblite/src/mal/optimizer/opt_projectionpath.h
        src/monetdblite/src/mal/optimizer/opt_aliases.h
        src/monetdblite/src/mal/optimizer/opt_profiler.h
        src/monetdblite/src/mal/optimizer/opt_costModel.h
        src/monetdblite/src/mal/optimizer/opt_constants.h
        src/monetdblite/src/mal/optimizer/opt_mergetable.h
        src/monetdblite/src/mal/optimizer/opt_macro.h
        src/monetdblite/src/mal/optimizer/opt_pipes.h
        src/monetdblite/src/mal/optimizer/opt_evaluate.h
        src/monetdblite/src/mal/optimizer/opt_deadcode.h
        src/monetdblite/src/mal/optimizer/opt_candidates.h
        src/monetdblite/src/mal/optimizer/opt_matpack.h
        src/monetdblite/src/mal/optimizer/opt_generator.h
        src/monetdblite/src/mal/optimizer/opt_inline.h
        src/monetdblite/src/mal/optimizer/opt_pushselect.h
        src/monetdblite/src/mal/optimizer/opt_prelude.h
        src/monetdblite/src/mal/optimizer/opt_multiplex.h
        src/monetdblite/src/mal/optimizer/opt_reorder.h
        src/monetdblite/src/mal/optimizer/opt_support.h
        src/monetdblite/src/mal/optimizer/opt_emptybind.h
        src/monetdblite/src/mal/optimizer/opt_commonTerms.h
        src/monetdblite/src/mal/optimizer/opt_dataflow.h
        src/monetdblite/src/mal/optimizer/opt_remap.h
        src/monetdblite/src/mal/optimizer/optimizer.h
        src/monetdblite/src/mal/optimizer/opt_coercion.h
        src/monetdblite/src/mal/optimizer/optimizer_private.h
        src/monetdblite/src/mal/sqlbackend/sql_gencode.h
        src/monetdblite/src/mal/sqlbackend/sql_statement.h
        src/monetdblite/src/mal/sqlbackend/sql_orderidx.h
        src/monetdblite/src/mal/sqlbackend/sql_assert.h
        src/monetdblite/src/mal/sqlbackend/sql_cast_impl_down_from_flt.h
        src/monetdblite/src/mal/sqlbackend/sql_user.h
        src/monetdblite/src/mal/sqlbackend/sql_upgrades.h
        src/monetdblite/src/mal/sqlbackend/sql_execute.h
        src/monetdblite/src/mal/sqlbackend/sql_cat.h
        src/monetdblite/src/mal/sqlbackend/sql_statistics.h
        src/monetdblite/src/mal/sqlbackend/sql_cast_impl_up_to_flt.h
        src/monetdblite/src/mal/sqlbackend/sql_transaction.h
        src/monetdblite/src/mal/sqlbackend/sql_round_impl.h
        src/monetdblite/src/mal/sqlbackend/mal_backend.h
        src/monetdblite/src/mal/sqlbackend/sql_optimizer.h
        src/monetdblite/src/mal/sqlbackend/rel_bin.h
        src/monetdblite/src/mal/sqlbackend/sql_fround_impl.h
        src/monetdblite/src/mal/sqlbackend/sql_cast.h
        src/monetdblite/src/mal/sqlbackend/sql_result.h
        src/monetdblite/src/mal/sqlbackend/sql_scenario.h
        src/monetdblite/src/mal/sqlbackend/sql_cast_impl_int.h
        src/monetdblite/src/mal/sqlbackend/sql_rank.h
        src/monetdblite/src/mal/sqlbackend/sql.h
        src/monetdblite/src/sql/include/sql_keyword.h
        src/monetdblite/src/sql/include/sql_mem.h
        src/monetdblite/src/sql/include/sql_list.h
        src/monetdblite/src/sql/include/sql_query.h
        src/monetdblite/src/sql/include/sql_hash.h
        src/monetdblite/src/sql/include/sql_relation.h
        src/monetdblite/src/sql/include/sql_catalog.h
        src/monetdblite/src/sql/include/sql_stack.h
        src/monetdblite/src/sql/common/sql_backend.h
        src/monetdblite/src/sql/common/sql_types.h
        src/monetdblite/src/sql/common/sql_string.h
        src/monetdblite/src/sql/server/rel_select.h
        src/monetdblite/src/sql/server/rel_planner.h
        src/monetdblite/src/sql/server/rel_dump.h
        src/monetdblite/src/sql/server/sql_parser.tab.h
        src/monetdblite/src/sql/server/rel_sequence.h
        src/monetdblite/src/sql/server/sql_privileges.h
        src/monetdblite/src/sql/server/rel_semantic.h
        src/monetdblite/src/sql/server/sql_atom.h
        src/monetdblite/src/sql/server/rel_exp.h
        src/monetdblite/src/sql/server/sql_mvc.h
        src/monetdblite/src/sql/server/rel_rel.h
        src/monetdblite/src/sql/server/rel_optimizer.h
        src/monetdblite/src/sql/server/rel_psm.h
        src/monetdblite/src/sql/server/rel_xml.h
        src/monetdblite/src/sql/server/sql_symbol.h
        src/monetdblite/src/sql/server/sql_datetime.h
        src/monetdblite/src/sql/server/sql_parser.h
        src/monetdblite/src/sql/server/rel_prop.h
        src/monetdblite/src/sql/server/sql_semantic.h
        src/monetdblite/src/sql/server/rel_schema.h
        src/monetdblite/src/sql/server/rel_updates.h
        src/monetdblite/src/sql/server/rel_remote.h
        src/monetdblite/src/sql/server/sql_scan.h
        src/monetdblite/src/sql/server/sql_decimal.h
        src/monetdblite/src/sql/server/rel_trans.h
        src/monetdblite/src/sql/server/rel_distribute.h
        src/monetdblite/src/sql/server/rel_partition.h
        src/monetdblite/src/sql/server/sql_qc.h
        src/monetdblite/src/sql/server/sql_env.h
        src/monetdblite/src/sql/storage/store_sequence.h
        src/monetdblite/src/sql/storage/sql_storage.h
        src/monetdblite/src/sql/storage/store_dependency.h
        src/monetdblite/src/sql/storage/bat/bat_utils.h
        src/monetdblite/src/sql/storage/bat/bat_table.h
        src/monetdblite/src/sql/storage/bat/bat_storage.h
        src/monetdblite/src/sql/storage/bat/bat_logger.h
        src/monetdblite/src/sql/storage/bat/nop_logger.h
        src/monetdblite/src/sql/storage/bat/res_table.h
        src/monetdblite/src/embedded/embedded.h
        src/embeddedjava/jni.h
        src/embeddedjava/embeddedjvm.h
        src/embeddedjava/jresulset.h
        src/embeddedjava/javaids.h
        src/embeddedjava/converters.h
        src/embeddedjava/checknulls.h
        src/embeddedjava/nl_cwi_monetdb_embedded_jdbc_EmbeddedDataBlockResponse.h
        src/embeddedjava/nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection.h
        src/embeddedjava/nl_cwi_monetdb_embedded_resultset_QueryResultSet.h
        src/embeddedjava/nl_cwi_monetdb_embedded_mapping_NullMappings.h
        src/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection.h
        src/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedPreparedStatement.h
        src/embeddedjava/nl_cwi_monetdb_embedded_tables_MonetDBTable.h
        src/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase.h
        src/monetdblite/src/embedded/embedded.c
        src/monetdblite/src/common/stream.c
        src/monetdblite/src/common/mutils.c
        src/monetdblite/src/gdk/gdk_aggr.c
        src/monetdblite/src/gdk/gdk_align.c
        src/monetdblite/src/gdk/gdk_atoms.c
        src/monetdblite/src/gdk/gdk_bat.c
        src/monetdblite/src/gdk/gdk_batop.c
        src/monetdblite/src/gdk/gdk_bbp.c
        src/monetdblite/src/gdk/gdk_calc.c
        src/monetdblite/src/gdk/gdk_cross.c
        src/monetdblite/src/gdk/gdk_delta.c
        src/monetdblite/src/gdk/gdk_firstn.c
        src/monetdblite/src/gdk/gdk_group.c
        src/monetdblite/src/gdk/gdk_hash.c
        src/monetdblite/src/gdk/gdk_heap.c
        src/monetdblite/src/gdk/gdk_imprints.c
        src/monetdblite/src/gdk/gdk_join.c
        src/monetdblite/src/gdk/gdk_logger.c
        src/monetdblite/src/gdk/gdk_orderidx.c
        src/monetdblite/src/gdk/gdk_posix.c
        src/monetdblite/src/gdk/gdk_project.c
        src/monetdblite/src/gdk/gdk_qsort.c
        src/monetdblite/src/gdk/gdk_sample.c
        src/monetdblite/src/gdk/gdk_search.c
        src/monetdblite/src/gdk/gdk_select.c
        src/monetdblite/src/gdk/gdk_ssort.c
        src/monetdblite/src/gdk/gdk_storage.c
        src/monetdblite/src/gdk/gdk_system.c
        src/monetdblite/src/gdk/gdk_tm.c
        src/monetdblite/src/gdk/gdk_unique.c
        src/monetdblite/src/gdk/gdk_utils.c
        src/monetdblite/src/gdk/gdk_value.c
        src/monetdblite/src/mal/mal/mal.c
        src/monetdblite/src/mal/mal/mal_atom.c
        src/monetdblite/src/mal/mal/mal_builder.c
        src/monetdblite/src/mal/mal/mal_client.c
        src/monetdblite/src/mal/mal/mal_dataflow.c
        src/monetdblite/src/mal/mal/mal_exception.c
        src/monetdblite/src/mal/mal/mal_function.c
        src/monetdblite/src/mal/mal/mal_import.c
        src/monetdblite/src/mal/mal/mal_instruction.c
        src/monetdblite/src/mal/mal/mal_interpreter.c
        src/monetdblite/src/mal/mal/mal_linker.c
        src/monetdblite/src/mal/mal/mal_listing.c
        src/monetdblite/src/mal/mal/mal_module.c
        src/monetdblite/src/mal/mal/mal_namespace.c
        src/monetdblite/src/mal/mal/mal_parser.c
        src/monetdblite/src/mal/mal/mal_resolve.c
        src/monetdblite/src/mal/mal/mal_runtime.c
        src/monetdblite/src/mal/mal/mal_scenario.c
        src/monetdblite/src/mal/mal/mal_session.c
        src/monetdblite/src/mal/mal/mal_stack.c
        src/monetdblite/src/mal/mal/mal_type.c
        src/monetdblite/src/mal/mal/mal_utils.c
        src/monetdblite/src/mal/modules/aggr.c
        src/monetdblite/src/mal/modules/algebra.c
        src/monetdblite/src/mal/modules/bat5.c
        src/monetdblite/src/mal/modules/batcalc.c
        src/monetdblite/src/mal/modules/batExtensions.c
        src/monetdblite/src/mal/modules/batmmath.c
        src/monetdblite/src/mal/modules/batstr.c
        src/monetdblite/src/mal/modules/blob.c
        src/monetdblite/src/mal/modules/calc.c
        src/monetdblite/src/mal/modules/group.c
        src/monetdblite/src/mal/modules/iterator.c
        src/monetdblite/src/mal/modules/language.c
        src/monetdblite/src/mal/modules/manifold.c
        src/monetdblite/src/mal/modules/mat.c
        src/monetdblite/src/mal/modules/mkey.c
        src/monetdblite/src/mal/modules/mmath.c
        src/monetdblite/src/mal/modules/mtime.c
        src/monetdblite/src/mal/modules/orderidx.c
        src/monetdblite/src/mal/modules/pcre.c
        src/monetdblite/src/mal/modules/projectionpath.c
        src/monetdblite/src/mal/modules/sample.c
        src/monetdblite/src/mal/modules/str.c
        src/monetdblite/src/mal/modules/tablet.c
        src/monetdblite/src/mal/optimizer/opt_aliases.c
        src/monetdblite/src/mal/optimizer/opt_candidates.c
        src/monetdblite/src/mal/optimizer/opt_coercion.c
        src/monetdblite/src/mal/optimizer/opt_commonTerms.c
        src/monetdblite/src/mal/optimizer/opt_constants.c
        src/monetdblite/src/mal/optimizer/opt_costModel.c
        src/monetdblite/src/mal/optimizer/opt_dataflow.c
        src/monetdblite/src/mal/optimizer/opt_deadcode.c
        src/monetdblite/src/mal/optimizer/opt_emptybind.c
        src/monetdblite/src/mal/optimizer/opt_evaluate.c
        src/monetdblite/src/mal/optimizer/opt_garbageCollector.c
        src/monetdblite/src/mal/optimizer/opt_generator.c
        src/monetdblite/src/mal/optimizer/opt_inline.c
        src/monetdblite/src/mal/optimizer/opt_macro.c
        src/monetdblite/src/mal/optimizer/opt_matpack.c
        src/monetdblite/src/mal/optimizer/opt_mergetable.c
        src/monetdblite/src/mal/optimizer/opt_mitosis.c
        src/monetdblite/src/mal/optimizer/opt_multiplex.c
        src/monetdblite/src/mal/optimizer/opt_pipes.c
        src/monetdblite/src/mal/optimizer/opt_prelude.c
        src/monetdblite/src/mal/optimizer/opt_profiler.c
        src/monetdblite/src/mal/optimizer/opt_projectionpath.c
        src/monetdblite/src/mal/optimizer/opt_pushselect.c
        src/monetdblite/src/mal/optimizer/opt_remap.c
        src/monetdblite/src/mal/optimizer/opt_reorder.c
        src/monetdblite/src/mal/optimizer/opt_support.c
        src/monetdblite/src/mal/optimizer/opt_wrapper.c
        src/monetdblite/src/mal/optimizer/optimizer.c
        src/monetdblite/src/mal/sqlbackend/mal_backend.c
        src/monetdblite/src/mal/sqlbackend/rel_bin.c
        src/monetdblite/src/mal/sqlbackend/sql.c
        src/monetdblite/src/mal/sqlbackend/sql_assert.c
        src/monetdblite/src/mal/sqlbackend/sql_bat2time.c
        src/monetdblite/src/mal/sqlbackend/sql_cast.c
        src/monetdblite/src/mal/sqlbackend/sql_cat.c
        src/monetdblite/src/mal/sqlbackend/sql_execute.c
        src/monetdblite/src/mal/sqlbackend/sql_fround.c
        src/monetdblite/src/mal/sqlbackend/sql_gencode.c
        src/monetdblite/src/mal/sqlbackend/sql_optimizer.c
        src/monetdblite/src/mal/sqlbackend/sql_orderidx.c
        src/monetdblite/src/mal/sqlbackend/sql_rank.c
        src/monetdblite/src/mal/sqlbackend/sql_result.c
        src/monetdblite/src/mal/sqlbackend/sql_round.c
        src/monetdblite/src/mal/sqlbackend/sql_scenario.c
        src/monetdblite/src/mal/sqlbackend/sql_statement.c
        src/monetdblite/src/mal/sqlbackend/sql_statistics.c
        src/monetdblite/src/mal/sqlbackend/sql_transaction.c
        src/monetdblite/src/mal/sqlbackend/sql_upgrades.c
        src/monetdblite/src/mal/sqlbackend/sql_user.c
        src/monetdblite/src/sql/common/sql_backend.c
        src/monetdblite/src/sql/common/sql_changeset.c
        src/monetdblite/src/sql/common/sql_hash.c
        src/monetdblite/src/sql/common/sql_keyword.c
        src/monetdblite/src/sql/common/sql_list.c
        src/monetdblite/src/sql/common/sql_mem.c
        src/monetdblite/src/sql/common/sql_stack.c
        src/monetdblite/src/sql/common/sql_string.c
        src/monetdblite/src/sql/common/sql_types.c
        src/monetdblite/src/sql/server/rel_distribute.c
        src/monetdblite/src/sql/server/rel_dump.c
        src/monetdblite/src/sql/server/rel_exp.c
        src/monetdblite/src/sql/server/rel_optimizer.c
        src/monetdblite/src/sql/server/rel_partition.c
        src/monetdblite/src/sql/server/rel_planner.c
        src/monetdblite/src/sql/server/rel_prop.c
        src/monetdblite/src/sql/server/rel_psm.c
        src/monetdblite/src/sql/server/rel_rel.c
        src/monetdblite/src/sql/server/rel_remote.c
        src/monetdblite/src/sql/server/rel_schema.c
        src/monetdblite/src/sql/server/rel_select.c
        src/monetdblite/src/sql/server/rel_semantic.c
        src/monetdblite/src/sql/server/rel_sequence.c
        src/monetdblite/src/sql/server/rel_trans.c
        src/monetdblite/src/sql/server/rel_updates.c
        src/monetdblite/src/sql/server/rel_xml.c
        src/monetdblite/src/sql/server/sql_atom.c
        src/monetdblite/src/sql/server/sql_datetime.c
        src/monetdblite/src/sql/server/sql_decimal.c
        src/monetdblite/src/sql/server/sql_env.c
        src/monetdblite/src/sql/server/sql_mvc.c
        src/monetdblite/src/sql/server/sql_parser.tab.c
        src/monetdblite/src/sql/server/sql_privileges.c
        src/monetdblite/src/sql/server/sql_qc.c
        src/monetdblite/src/sql/server/sql_scan.c
        src/monetdblite/src/sql/server/sql_semantic.c
        src/monetdblite/src/sql/server/sql_symbol.c
        src/monetdblite/src/sql/storage/bat/bat_logger.c
        src/monetdblite/src/sql/storage/bat/bat_storage.c
        src/monetdblite/src/sql/storage/bat/bat_table.c
        src/monetdblite/src/sql/storage/bat/bat_utils.c
        src/monetdblite/src/sql/storage/bat/nop_logger.c
        src/monetdblite/src/sql/storage/bat/res_table.c
        src/monetdblite/src/sql/storage/sql_catalog.c
        src/monetdblite/src/sql/storage/store.c
        src/monetdblite/src/sql/storage/store_dependency.c
        src/monetdblite/src/sql/storage/store_sequence.c
        src/embeddedjava/converters.c
        src/embeddedjava/checknulls.c
        src/embeddedjava/embeddedjvm.c
        src/embeddedjava/javaids.c
        src/embeddedjava/jresultset.c
        src/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection.c
        src/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase.c
        src/embeddedjava/nl_cwi_monetdb_embedded_env_MonetDBEmbeddedPreparedStatement.c
        src/embeddedjava/nl_cwi_monetdb_embedded_jdbc_EmbeddedDataBlockResponse.c
        src/embeddedjava/nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection.c
        src/embeddedjava/nl_cwi_monetdb_embedded_mapping_NullMappings.c
        src/embeddedjava/nl_cwi_monetdb_embedded_resultset_QueryResultSet.c
        src/embeddedjava/nl_cwi_monetdb_embedded_tables_MonetDBTable.c
)

add_dependencies(monetdb5 sqlparser inlines)

if (UNIX)
    set_target_properties(monetdb5 PROPERTIES LINK_FLAGS "-lm -lrt -ldl -lpthread")
endif (UNIX)

if (APPLE)
    set_target_properties(monetdb5 PROPERTIES LINK_FLAGS "-ldl -lpthread")
endif (APPLE)
