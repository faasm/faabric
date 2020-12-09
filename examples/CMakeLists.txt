cmake_minimum_required(VERSION 3.13.0)
project(faabric-examples)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# -----------------------------------------------
# These examples must support being compiled as stand-alone project.
# Don't include them in the main CMake build, instead link them against 
# the installed Faabric library as an external user would.
# -----------------------------------------------

add_executable(helloworld hello_world.cpp)
target_link_libraries(helloworld libfaabric.so)

set(ALL_EXAMPLES 
    helloworld
    )

add_custom_target(all_examples DEPENDS ${ALL_EXAMPLES})
