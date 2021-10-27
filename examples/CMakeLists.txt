cmake_minimum_required(VERSION 3.21.0)
project(faabric-examples)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# -----------------------------------------------
# These examples must support being compiled as stand-alone project.
# Don't include them in the main CMake build, instead link them against
# the installed Faabric library as an external user would.
# -----------------------------------------------
set(FAABRIC_LIB_DIR "/build/faabric/install/lib")

include(../cmake/ExternalProjects.cmake)

function(add_example example_name)
    add_executable(${example_name} ${example_name}.cpp)

    target_link_libraries(${example_name}
        ${FAABRIC_LIB_DIR}/libfaabric.so
        ${FAABRIC_LIB_DIR}/libfaabricmpi.so
        faabric::common_dependencies
    )

    set_property(TARGET ${example_name} PROPERTY BUILD_RPATH "${FAABRIC_LIB_DIR}")

    set(ALL_EXAMPLES ${ALL_EXAMPLES} ${example_name} PARENT_SCOPE)
endfunction()

add_example(check)
add_example(server)

add_custom_target(all_examples DEPENDS ${ALL_EXAMPLES})
