#ifndef CONFIG_H
#define CONFIG_H
#pragma once

//#include <fstream>
//#include <iostream>
#include <string>
#include <nlohmann/json.hpp>

enum class RunType {
    Simple = 0,
    Proxy = 1,
    BindInproc = 2,
    ConnectInproc = 3,
    LockFreePush = 4,
    LockFreeDealer = 5,
    LockFreeReply = 6,
    TestOut = 7,
    TestIn = 8,
};

NLOHMANN_JSON_SERIALIZE_ENUM(RunType, {
    {RunType::Simple, "Simple"},
    {RunType::Proxy, "Proxy"},
    {RunType::BindInproc, "BindInproc"},
    {RunType::ConnectInproc, "ConnectInproc"},
    {RunType::LockFreePush, "LockFreePush"},
    {RunType::LockFreeDealer, "LockFreeDealer"},
    {RunType::LockFreeReply, "LockFreeReply"},
    {RunType::TestOut, "TestOut"},
    {RunType::TestIn, "TestIn"}
})

struct Config
{
    std::string inurl;                            // ZMQ url for incoming messages
    std::string outurl;                           // ZMQ url for outgoing messages
    std::string workerurl = "inproc://worker";    // internal url, inproc modes (type 2/3) only
    RunType type;                                 // pipeline mode
    unsigned helper_threads;                      // extra sender threads, inproc modes only
    int io_threads;                               // zmq background i/o threads in the context
    int hwm;                                      // zmq msg cap (high water mark)
    int timeout;                                  // receive timeout in ms, -1 blocks forever
    bool verbose;                                 // verbose mode to show queue depth once a second
    bool metrics;                                 // send metrics counters to fastcache api
    uint32_t metrics_interval;                    // time between metrics publications
    uint32_t cache_id;                            // makes the metrics ipc path unique per instance
    uint16_t simdata_size_mb;                     // simulated data size in MB (type 7 only)
    uint16_t datasim_threads;                     // number of threads that simulate data (type 7 only)
};

void load_config(const std::string& fname, Config& conf);

#endif
