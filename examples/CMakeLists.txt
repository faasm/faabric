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
set(FAABRIC_LIB_DIR "/build/faabric/install/lib")

function(add_example example_name)
    add_executable(${example_name} ${example_name}.cpp)

    target_link_libraries(${example_name}
        ${FAABRIC_LIB_DIR}/libfaabric.so
        ${FAABRIC_LIB_DIR}/libfaabricmpi.so
        ${FAABRIC_LIB_DIR}/libprotobuf.so
        ${FAABRIC_LIB_DIR}/libpistache.so
        ${FAABRIC_LIB_DIR}/libzmq.so
        boost_system
        boost_filesystem
        hiredis
        pthread
    )

    set(ALL_EXAMPLES ${ALL_EXAMPLES} ${example_name} PARENT_SCOPE)
endfunction()

add_example(check)
add_example(server)

add_custom_target(all_examples DEPENDS ${ALL_EXAMPLES})
