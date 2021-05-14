file(GLOB_RECURSE TEST_FILES ${CMAKE_CURRENT_LIST_DIR} test_*.cpp)

include_directories(
    ${CMAKE_CURRENT_LIST_DIR}
    ${CMAKE_CURRENT_LIST_DIR}/../utils
)

add_executable(
    faabric_dist_tests
    main.cpp
    ${TEST_FILES}
)

add_executable(
    faabric_dist_test_server
    server.cpp
)

target_link_libraries(faabric_dist_tests
    faabric_test_utils
)

target_link_libraries(faabric_dist_test_server
    faabric_test_utils
)

add_test(NAME faabric_dist_tests COMMAND "tests/dist/faabric_dist_tests")

