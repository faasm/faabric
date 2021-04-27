# ----------------------------------------------
# ZeroMQ with CPP bindings over protobuf
# ----------------------------------------------

set(PB_SRC "${CMAKE_CURRENT_BINARY_DIR}/mpiMessage.pb.cc")
set(PB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/mpiMessage.pb.h")
set(PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/zeromq/mpiMessage.pb.h")

add_custom_command(
    OUTPUT "${PB_HEADER}" "${PB_SRC}"
    DEPENDS mpiMessage.proto
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    COMMAND protoc
    ARGS -I "${CMAKE_CURRENT_LIST_DIR}"
    --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
    mpiMessage.proto
)

add_custom_command(
    OUTPUT "${PB_HEADER_COPIED}"
    DEPENDS "${PB_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${PB_HEADER}
    ${PB_HEADER_COPIED}
)

# ----------------------------------------------
# Faabric library
# ----------------------------------------------

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/zeromq/*.h")

set(LIB_FILES 
    ${PB_HEADER_COPIED}
    ${PB_SRC}
    MessageEndpoint.cpp
    MpiMessageEndpoint.cpp
    ${HEADERS}
)

faabric_lib(zeromq "${LIB_FILES}")

# Add include directories for zeromq themselves
target_include_directories(zeromq PUBLIC ${ZEROMQ_INCLUDE_DIRS})

# ----------------------------------------------
# Link with ZeroMQ
# ----------------------------------------------

add_dependencies(zeromq libzeromq_ext)
add_dependencies(zeromq cppzeromq_ext)

target_link_libraries(zeromq ${Protobuf_LIBRARIES} scheduler zmq)

