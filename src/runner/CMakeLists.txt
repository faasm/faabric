set(LIB_FILES
        "${FAABRIC_INCLUDE_DIR}/faabric/runner/FaabricMain.h"
        FaabricMain.cpp
        )

faabric_lib(runner "${LIB_FILES}")

target_link_libraries(runner scheduler util endpoint)

add_executable(pagetest pagemap_test.cpp)
target_link_libraries(pagetest scheduler util endpoint)

