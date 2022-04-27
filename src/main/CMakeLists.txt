set(BYTE_YOUNG_SRC
  main.cpp
  ddl.cpp
  executor.cpp
  index.cpp
  metadata.cpp
  optimizer.cpp
  parser.cpp
  storage.cpp
  trx.cpp
  util.cpp
)

add_executable(byte_young
  ${BYTE_YOUNG_SRC})

target_link_libraries(byte_young
  ${CMAKE_SOURCE_DIR}/sql-parser/lib/libsqlparser.so)