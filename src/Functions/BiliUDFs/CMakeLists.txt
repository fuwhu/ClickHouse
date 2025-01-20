include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_bili_udf .)
add_library(clickhouse_functions_bili_udf OBJECT ${clickhouse_functions_bili_udf_sources} ${clickhouse_functions_bili_udf_headers})
target_link_libraries(clickhouse_functions_bili_udf PRIVATE dbms)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_bili_udf PRIVATE "-g0")
endif()
