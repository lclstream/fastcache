#ifndef CACHESERVER_H
#define CACHESERVER_H
#pragma once

#include <zmq.h>
#include "config.h"
#include <boost/lockfree/spsc_queue.hpp>

class CacheServer {

public:
    CacheServer(const Config& cfg);
    ~CacheServer();
    void Run();

private:
    static void* proxy_worker(void *arg);
    static void* sending_worker(void* arg);
    static void* request_handler(void* arg);
    static void* lockfree_receiver_worker2(void* arg);
    static void* lockfree_receiver_worker(void* arg);
    static void* lockfree_sender_worker(void* arg);
    static void* inproc_receiver_worker(void* arg);
    static void* inproc_sending_worker(void* arg);
    static int send_receive(void* incoming, void* outgoing);
    static void* test_connection(void* arg);
    void cpulock(int num);

private:
    int num_workers = 0;
    int helper_threads;
    int hwm;
    std::string outurl;
    std::string inurl;
    const std::string workerurl = "inproc://workers";
    std::string requrl;
    //zmq::context_t zmq_ctx;
    void *zmq_ctx;
    std::vector<pthread_t> workers;
    zmq_msg_t message_cache;
    bool has_cache = false;
    pthread_mutex_t cache_lock;
    int type;
    bool verbose;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<25>> queue;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<25>> queue2;
};

#endif
