file(GLOB_RECURSE TEST_FILES ${CMAKE_CURRENT_LIST_DIR} test_*.cpp)

# Shared
add_library(faabric_dist_tests_lib
    init.h
    init.cpp
    DistTestExecutor.h
    DistTestExecutor.cpp
    scheduler/functions.cpp
    transport/functions.cpp
    )

target_link_libraries(faabric_dist_tests_lib PUBLIC faabric::test_utils)
target_include_directories(faabric_dist_tests_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Tests
add_executable(
    faabric_dist_tests
    main.cpp
    ${TEST_FILES}
)

target_link_libraries(faabric_dist_tests PRIVATE faabric_dist_tests_lib)

add_test(NAME faabric_dist_tests COMMAND "tests/dist/faabric_dist_tests")

# Server
add_executable(
    faabric_dist_test_server
    server.cpp
)

target_link_libraries(faabric_dist_test_server PRIVATE faabric_dist_tests_lib)
