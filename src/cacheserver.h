#ifndef CACHESERVER_H
#define CACHESERVER_H
#pragma once

#include <zmq.h>
#include "config.h"

class CacheServer {

public:
    CacheServer(const Config& cfg);
    ~CacheServer();
    void Run();

private:
    static void* proxy_worker(void *arg);

private:
    void *receiver_socket;
    void *distributor_socket;
    int num_workers;
    int hwm;
    std::string outurl;
    const std::string workerurl = "inproc://workers";
    //zmq::context_t zmq_ctx;
    void * zmq_ctx;
    std::vector<pthread_t> workers;
};

#endif
