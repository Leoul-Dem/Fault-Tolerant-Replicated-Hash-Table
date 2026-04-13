cmake_minimum_required(VERSION 3.16)
project(Replicated_Hash_Table)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -pthread")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static")

include_directories(libs)
include_directories(libs/ds)
include_directories(libs/ds/hashmap)
include_directories(libs/networking)

add_executable(Replicated_Hash_Table
        src/Node.cpp
        src/Raft.cpp
        src/main.cpp
)

target_link_libraries(Replicated_Hash_Table pthread)
