set(SQL_PARSER_TEST_SRC
  main.cc
)

add_executable(sql-parser-test
  ${SQL_PARSER_TEST_SRC})

target_link_libraries(sql-parser-test
  ${CMAKE_SOURCE_DIR}/sql-parser/lib/libsqlparser.so)