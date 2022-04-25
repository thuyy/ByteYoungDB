cmake_minimum_required(VERSION 3.8)
project(ByteYoungDB)
set(CMAKE_CXX_STANDARD 11)                      # c++11
set(CMAKE_CXX_COMPILER "clang++" )              # 显示指定使用的C++编译器
set(CMAKE_CXX_FLAGS "-g")                       # 调试信息
set(CMAKE_CXX_FLAGS "-Wall")                    # 开启所有警告
set(CMAKE_CXX_FLAGS_DEBUG "-O0" )               # 调试包不优化
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG " )   # release包优化

include(GNUInstallDirs)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR})

include_directories(${CMAKE_SOURCE_DIR}/sql-parser/include)

add_subdirectory(src/main)
add_subdirectory(src/sql-parser-test)