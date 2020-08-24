include_directories(${FAABRIC_INCLUDE_DIR}/faabric/redis)

set(LIB_FILES
        Redis.cpp
        ${FAABRIC_INCLUDE_DIR}/faabric/redis/Redis.h
    )

add_library(redis ${LIB_FILES})

target_link_libraries(redis util pthread hiredis)
