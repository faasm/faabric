faabric_lib(batch_scheduler
    BatchScheduler.cpp
    BinPackScheduler.cpp
)

target_link_libraries(batch_scheduler PRIVATE
    faabric::util
)
