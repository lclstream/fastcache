cmake_minimum_required(VERSION 3.20)

project(lclstream-fastcache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ZeroMQ CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS lockfree)

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
)
