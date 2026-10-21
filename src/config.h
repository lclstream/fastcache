#ifndef CONFIG_H
#define CONFIG_H
#pragma once

#include <string>
#include <nlohmann/json.hpp>
#include <e2sar.hpp>

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
    ZMQPULLinEJFatOut = 9,
    TestInEJFatOut = 10,
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
    {RunType::TestIn, "TestIn"},
    {RunType::ZMQPULLinEJFatOut, "ZMQPULLinEJFatOut"},
    {RunType::TestInEJFatOut, "TestInEJFatOut"},
})

struct Config
{
    std::string inurl;                            // ZMQ url for incoming messages
    std::string outurl;                           // ZMQ url for outgoing messages
    std::string workerurl = "inproc://worker";    // internal url, inproc modes (type 2/3) only
    RunType type;                                 // pipeline mode
    unsigned helper_threads;                      // extra sender threads, inproc modes only
    int zmq_io_threads;                           // zmq background i/o threads in the context
    int hwm;                                      // zmq msg cap (high water mark)
    int timeout;                                  // receive timeout in ms, -1 blocks forever
    bool verbose;                                 // verbose mode to show queue depth once a second

    bool metrics;                                 // send metrics counters to fastcache api
    uint32_t metrics_interval;                    // time between metrics publications
    uint32_t metrics_cache_id;                    // makes the metrics ipc path unique per instance

    bool ejfat_useLB = true;                      // Use load balancer
    bool ejfat_useIPv6 = false;                   // Use IPv6
    uint16_t ejfat_mtu;                           // interface mtu
    int ejfat_socketbufsize;                      // send/receive buffer size (kernel doubles it, so don't set max)
    float ejfat_rateGbps;                         // speed limit (can cause small bursts)
    size_t ejfat_numSendSockets;                  // ejfat send sockets
    uint16_t ejfat_dataId;                        // data id (note: might want to have this loop at some point)
    uint16_t ejfat_queueSize;                     // ejfat internal lockfree queue size
    bool ejfat_reportstats = true;                // report ejfat stats back to LB
    //uint16_t dataSimulatorThreads;                // number of threads simulating data
    std::string ejfat_uri = "";                   // ejfat instance URI obtained from the load balancer
    uint16_t simdata_size_mb;                              // simulated message size
    size_t ejfat_recvthreads;                     // number of ejfat reassembler threads
    //size_t ejfat_readthreads;                     // number of ejfat read threads
    int ejfat_eventtimeoutms;                     // Timeout for event reassembly (ms)
};

void load_config(const std::string& fname, Config& conf);

#endif
