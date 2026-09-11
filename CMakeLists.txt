cmake_minimum_required(VERSION 3.20)

project(lclstream-fastcache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ZeroMQ CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS lockfree url program_options chrono thread filesystem log)
find_package(protobuf REQUIRED)
find_package(gRPC REQUIRED)

find_path(EJFAT_INCDIR
    NAMES e2sar.hpp
    PATHS $ENV{EJFAT_PATH}/include
    NO_DEFAULT_PATH
)

find_library(EJFAT_LIB
    NAMES e2sar
    PATHS $ENV{EJFAT_PATH}/lib64
    NO_DEFAULT_PATH
)

if(EJFAT_INCDIR AND EJFAT_LIB)
    add_library(e2sar::e2sar UNKNOWN IMPORTED)
    set_target_properties(e2sar::e2sar PROPERTIES
        IMPORTED_LOCATION "${EJFAT_LIB}"
        INTERFACE_INCLUDE_DIRECTORIES "${EJFAT_INCDIR}"
        INTERFACE_LINK_LIBRARIES "Boost::log;Boost::thread;Boost::chrono;Boost::url;gRPC::grpc++;protobuf::libprotobuf"
    )
endif()

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
    uring
)


enable_testing()

add_executable(threadworker_test
    tests/test_threadworker.cpp
    src/threadworker.cpp
)

target_link_libraries(threadworker_test
    PRIVATE
    libzmq
    nlohmann_json::nlohmann_json
    Boost::lockfree
    Boost::thread
    Boost::chrono
    e2sar::e2sar
    uring
)

add_test(
    NAME threadworker_test
    COMMAND threadworker_test
)

add_executable(lockfreeworker_test
    tests/test_lockfreeworker.cpp
    src/threadworker.cpp
)

target_link_libraries(lockfreeworker_test
    PRIVATE
    libzmq
    nlohmann_json::nlohmann_json
    Boost::lockfree
    Boost::thread
    Boost::chrono
    e2sar::e2sar
    uring
)

add_test(
    NAME lockfreeworker_test
    COMMAND lockfreeworker_test
)
