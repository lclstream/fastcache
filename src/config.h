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
    EJFatInZMQPUSHOut = 11,
    EJFatInZMQREPOut = 12,
    EJFatInZMQROUTEROut = 13,
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
    {RunType::EJFatInZMQPUSHOut, "EJFatInZMQPUSHOut"},
    {RunType::EJFatInZMQREPOut, "EJFatInZMQREPOut"},
    {RunType::EJFatInZMQROUTEROut, "EJFatInZMQROUTEROut"},
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
    uint16_t simdata_size_mb;                     // simulated message size

    bool metrics;                                 // send metrics counters to fastcache api
    uint32_t metrics_interval;                    // time between metrics publications
    uint32_t metrics_cache_id;                    // makes the metrics ipc path unique per instance

    bool ejfat_useLB = true;                      // Both: Use load balancer
    bool ejfat_useIPv6 = false;                   // Both: Use IPv6
    std::string ejfat_uri = "";                   // Both: ejfat instance URI obtained from the load balancer
    int ejfat_socketbufsize;                      // Both: send/receive buffer size (kernel doubles it, so don't set max)
    uint16_t ejfat_mtu;                           // Sender: interface mtu
    float ejfat_rateGbps;                         // Sender: speed limit (can cause small bursts)
    size_t ejfat_numSendSockets;                  // Sender: ejfat send sockets
    uint16_t ejfat_dataId;                        // Sender: data id (note: might want to have this loop at some point)
    uint16_t ejfat_queueSize;                     // Sender: ejfat internal lockfree queue size
    bool ejfat_reportstats = true;                // Receiver: report ejfat stats back to LB
    size_t ejfat_recvthreads;                     // Receiver: number of ejfat reassembler threads
    int ejfat_eventtimeoutms;                     // Receiver: Timeout for event reassembly (ms)
    //uint16_t dataSimulatorThreads;              // number of threads simulating data
    //size_t ejfat_readthreads;                   // number of ejfat read threads
};

void load_config(const std::string& fname, Config& conf);

#endif
