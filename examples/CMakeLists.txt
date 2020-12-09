cmake_minimum_required(VERSION 3.13.0)
project(faabricmpi)

# -----------------------------------------------
# These examples must support being compiled as stand-alone project,
# and should assume Faabric has been installed
# -----------------------------------------------

add_executable(helloworld hello_world.cpp)

target_link_libraries(helloworld faabric)

set(ALL_EXAMPLES 
    helloworld
    )

add_custom_target(all_examples DEPENDS ${ALL_EXAMPLES})
