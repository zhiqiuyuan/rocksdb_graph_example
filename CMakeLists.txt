cmake_minimum_required(VERSION 3.15)
project(rdb_example)

set(CMAKE_CXX_STANDARD 14)

add_executable(rdb_example main.cpp )
target_link_libraries(rdb_example librocksdb.a  -lpthread -llz4 -lsnappy -lbz2 -lzstd -ldl libz.so)