# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

# CMake version check
cmake_minimum_required(VERSION 3.5.0)

# Set the project name
project("Nebula C++ Client simple example" C CXX)

set(CMAKE_CXX_STANDARD 14)  # specify the C++ standard

set(NEBULA_HOME /usr/local/nebula)

# This project only depends on the nebula-client headers/library
include_directories(${NEBULA_HOME}/include)
link_directories(${NEBULA_HOME}/lib64)

add_executable(session_example
    SessionExample.cpp
)

target_link_libraries(session_example
    PRIVATE
        nebula_graph_client
)
