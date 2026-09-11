#ifndef CONFIG_H
#define CONFIG_H
#pragma once

#include <string>
#include <nlohmann/json.hpp>
#include <e2sar.hpp>

using json = nlohmann::json;

struct Config
{
    std::string inurl;
    std::string outurl;
    std::string workerurl = "inproc://worker";
    int type;
    unsigned helper_threads;
    int zmq_io_threads;
    int hwm;
    int timeout;
    bool verbose;
    bool metrics;
    uint32_t metrics_cache_id;
    uint32_t metrics_interval;
    bool ejfat_useLB = true;
    uint16_t ejfat_mtu;
    int ejfat_sndbufsize;
    float ejfat_rateGbps;
    size_t ejfat_numSendSockets;
    uint16_t ejfat_dataId;
    uint16_t dataSimulatorThreads;
    std::string ejfat_uri = "";
    uint16_t dataMB;
};

void load_config(const std::string& fname, Config& conf);

#endif
