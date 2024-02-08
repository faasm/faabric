CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
PROJECT(project)

set(CMAKE_CXX_STANDARD 20)
SET(CMAKE_BUILD_TYPE Release)

add_executable(bench_mpi_allreduce
    main.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../benchmarks/mpi_allreduce.cpp
)

target_compile_definitions(bench_mpi_allreduce PRIVATE USE_REAL_MPI=1)
target_link_libraries(bench_mpi_allreduce mpi)
