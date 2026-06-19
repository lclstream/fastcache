#ifndef CACHESERVER_H
#define CACHESERVER_H
#pragma once

#include <vector>
#include <memory>
#include <thread>
#include <zmq.h>
#include "config.h"
#include "threadworker.h"
#include <boost/lockfree/spsc_queue.hpp>

class CacheServer {

public:
    CacheServer(Config& cfg);
    ~CacheServer();
    void Run();

private:
    std::vector<std::unique_ptr<ThreadWorker>> create(Config& cfg, void* zmq_ctx);

private:
    Config cfg;
    void* zmq_ctx = nullptr;
    std::vector<std::unique_ptr<ThreadWorker>> workers;
    std::vector<std::thread> threads;
    bool verbose = false;
    MessageQueue queue;

};

#endif
