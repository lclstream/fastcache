#ifndef THREADWORKER_H
#define THREADWORKER_H
#pragma once

#include <string>
#include "config.h"
#include <boost/lockfree/spsc_queue.hpp>
#include <zmq.h>


using MessageQueue = boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>>;

struct SocketConfig {
    int type; // (push/pull)
    int hwm;
    std::string url;
    bool isbind;
};

class ThreadWorker {
public:
    ThreadWorker(void* zmq_ctx, const Config& config)
        : zmq_ctx(zmq_ctx), cfg(config) {};
    virtual ~ThreadWorker() = default;
    virtual void run() = 0;

protected:
    void* create_socket(void* ctx, const SocketConfig& socketcfg);
    int send_receive(void* incoming, void* outgoing);

protected:
    void *zmq_ctx;
    const Config cfg;
};

class ProxyWorker : public ThreadWorker {
public:
    ProxyWorker(void* ctx, const Config& cfg, bool proxy)
        : ThreadWorker(ctx, cfg), proxy(proxy) {};
    void run() override;
private:
    bool proxy;
};

class InprocWorker : public ThreadWorker {
public:
    InprocWorker(void* ctx, const Config& cfg, bool sender, bool bindoutgoing)
        : ThreadWorker(ctx, cfg), sender(sender), bindoutgoing(bindoutgoing) {};
    void run() override;
private:
    bool sender;
    bool bindoutgoing;
};

class LockfreeWorker : public ThreadWorker {
public:
    LockfreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        bool sender,
        std::atomic<bool>& shutdown
    ) : ThreadWorker(ctx, cfg), shutdown(shutdown),
                                sender(sender),
                                queue(queue),
                                timeout(cfg.timeout) {};
    void run() override;
protected:
    std::atomic<bool>& shutdown;
private:
    bool sender;
    MessageQueue& queue;
    int timeout;
    void* metrics_socket = nullptr;
};

class ConnectionTesterWorker : public ThreadWorker {
public:
    ConnectionTesterWorker(void* ctx, const Config& cfg)
        : ThreadWorker(ctx, cfg) {};
    void run() override;
};

#endif
