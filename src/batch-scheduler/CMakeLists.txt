faabric_lib(scheduling_util
    DecisionCache.cpp
    SchedulingDecision.cpp
)

faabric_lib(batch_scheduler
    BatchScheduler.cpp
    BinPackScheduler.cpp
    CompactScheduler.cpp
    SpotScheduler.cpp
)

target_link_libraries(batch_scheduler PRIVATE
    faabric::util
    faabric::scheduling_util
)
