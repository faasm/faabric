file(GLOB_RECURSE TEST_FILES ${CMAKE_CURRENT_LIST_DIR} test_*.cpp)

add_executable(
    faabric_tests
    main.cpp
    ${TEST_FILES}
)

target_include_directories(faabric_tests PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(faabric_tests PRIVATE
    faabric::test_utils
    faabric::common_dependencies
)

add_test(NAME faabric_tests COMMAND "tests/test/faabric_tests")
