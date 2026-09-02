cmake_minimum_required(VERSION 3.20)

project(lclstream-fastcache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ZeroMQ CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS lockfree url program_options chrono thread filesystem log)
find_package(protobuf REQUIRED)
find_package(gRPC REQUIRED)

include(FetchContent)

FetchContent_Declare(e2sar
    GIT_REPOSITORY https://github.com/JeffersonLab/E2SAR.git
    GIT_TAG main
)
FetchContent_MakeAvailable(e2sar)

add_executable(lclstream-fastcache
    src/fastcache.cpp
    src/cacheserver.cpp
    src/config.cpp
    src/threadworker.cpp
)

target_link_libraries(lclstream-fastcache
    PRIVATE
    libzmq
    nlohmann_json::nlohmann_json
    Boost::lockfree
    e2sar::e2sar
    gRPC::grpc++
)


enable_testing()

add_executable(threadworker_test
    tests/test_threadworker.cpp
)

target_link_libraries(threadworker_test
    PRIVATE
    libzmq
    nlohmann_json::nlohmann_json
)

add_test(
    NAME threadworker_test
    COMMAND threadworker_test
)

add_executable(lockfreeworker_test
    tests/test_lockfreeworker.cpp
)

target_link_libraries(lockfreeworker_test
    PRIVATE
    libzmq
    nlohmann_json::nlohmann_json
    Boost::lockfree
)

add_test(
    NAME lockfreeworker_test
    COMMAND lockfreeworker_test
)
