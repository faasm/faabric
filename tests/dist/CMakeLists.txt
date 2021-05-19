file(GLOB_RECURSE TEST_FILES ${CMAKE_CURRENT_LIST_DIR} test_*.cpp)

include_directories(
    ${CMAKE_CURRENT_LIST_DIR}
    ${CMAKE_CURRENT_LIST_DIR}/../utils
)

add_executable(
    faabric_dist_tests
    main.cpp
    DistTestExecutor.h
    DistTestExecutor.cpp
    ${TEST_FILES}
)

target_link_libraries(faabric_dist_tests
    faabric_test_utils
)

add_test(NAME faabric_dist_tests COMMAND "tests/dist/faabric_dist_tests")

add_subdirectory(server)

