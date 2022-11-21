include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_udf .)
add_library(clickhouse_functions_udf ${clickhouse_functions_udf_sources} ${clickhouse_functions_udf_headers})
target_link_libraries(clickhouse_functions_udf PRIVATE dbms)

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_udf PRIVATE "-g0")
endif()
