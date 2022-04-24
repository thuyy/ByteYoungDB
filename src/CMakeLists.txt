set(BYTE_YOUNG_SRC
  main.cc
)

add_executable(byte_young
  ${BYTE_YOUNG_SRC})

target_link_libraries(byte_young
  ${CMAKE_SOURCE_DIR}/sql-parser/lib/libsqlparser.so)