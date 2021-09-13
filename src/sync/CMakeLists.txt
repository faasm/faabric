set(LIB_FILES
        DistributedSync.cpp
        ${FAABRIC_INCLUDE_DIR}/faabric/sync/DistributedSync.h
    )

faabric_lib(sync "${LIB_FILES}")

target_link_libraries(sync scheduler)
