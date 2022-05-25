set(BYTE_YOUNG_SRC
  main.cpp
  executor.cpp
  index.cpp
  metadata.cpp
  optimizer.cpp
  parser.cpp
  storage.cpp
  trx.cpp
  util.cpp
)

add_executable(bydb
  ${BYTE_YOUNG_SRC})

target_link_libraries(bydb
  ${CMAKE_SOURCE_DIR}/sql-parser/lib/libsqlparser.so)