cmake_minimum_required(VERSION 3.13.0)
project(hellofunc)

include_directories(
        ${FAABRIC_INCLUDE_DIR}
)

add_executable(hellofunc
        hello.cpp
        )

target_link_libraries(hellofunc faabric)
