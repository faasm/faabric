CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
PROJECT(project)

set(CMAKE_CXX_STANDARD 20)
SET(CMAKE_BUILD_TYPE Release)

function(ADD_BENCH_TARGET target)
    add_executable(${target} ${ARGN})
    target_compile_definitions(${target} PRIVATE USE_REAL_MPI=1)
    target_link_libraries(${target} mpi)
endfunction()

ADD_BENCH_TARGET(bench_mpi_allreduce
    bench_mpi_allreduce.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../benchmarks/mpi_allreduce.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../benchmarks/mpi_bench.cpp
)

ADD_BENCH_TARGET(bench_mpi_send_recv
    bench_mpi_send_recv.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../benchmarks/mpi_send_recv.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../benchmarks/mpi_bench.cpp
)
