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

function(add_example example_name)
    add_executable(${example_name} ${example_name}.cpp)

    target_link_libraries(${example_name}
        faabric
        faabricmpi
        faabricmpi_native
        protobuf
        pthread
        pistache
        hiredis
        grpc++
        grpc++_reflection
        boost_system
        boost_filesystem
    )
    
    set(ALL_EXAMPLES ${ALL_EXAMPLES} ${example_name} PARENT_SCOPE)   
endfunction()

add_example(check)
add_example(mpi_helloworld)
add_example(mpi_send)
add_example(mpi_sendrecv)
add_example(server)

add_custom_target(all_examples DEPENDS ${ALL_EXAMPLES})
