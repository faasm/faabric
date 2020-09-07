add_executable(helloworld hello_world.cpp)

target_link_libraries(helloworld executor state endpoint)
