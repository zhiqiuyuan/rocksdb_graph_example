cmake_minimum_required(VERSION 3.15)
project(rdb_example)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} -std=c++11 -g -Wall -rdynamic -pthread")

add_executable(simple_rdb_eg simple_adjlist.cpp )
target_link_libraries(simple_rdb_eg librocksdb.a -lpthread -llz4 -lsnappy -lbz2 -lzstd -ldl libz.so)

add_executable(link_rdb_eg link_adjlist.cpp )
target_link_libraries(link_rdb_eg librocksdb.a -lpthread -llz4 -lsnappy -lbz2 -lzstd -ldl libz.so)