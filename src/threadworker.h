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

struct MetricsData {
    uint64_t rc_count = 0;
    uint64_t msg_count = 0;
    uint64_t metrics_count = 0;
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

class LockFreeWorker : public ThreadWorker {
public:
    LockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        bool sender,
        std::atomic<bool>& shutdown
    ) : ThreadWorker(ctx, cfg), shutdown(shutdown),
                                sender(sender),
                                queue(queue),
                                timeout(cfg.timeout) {};
    //void run() override;
protected:
    std::atomic<bool>& shutdown;
    bool sender;
    MessageQueue& queue;
    int timeout;
protected:
    std::string create_metrics(uint64_t rc_count, uint64_t msg_count, uint64_t metrics_count);
    void send_metrics(MetricsData& metrics, int rc, void* metrics_socket);
    void* create_metrics_socket(std::string& metrics_path);
    void cleanup_metrics(std::thread::id tid, void* metrics_socket, std::string& metrics_path);
};

class SenderLockFreeWorker : public LockFreeWorker {
public:
    SenderLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown,
        int socket_type = ZMQ_PUSH
    ) : LockFreeWorker(ctx, cfg, queue, true, shutdown), socket_type(socket_type) {};
    void run() override;
private:
    int socket_type;
};

class ReceiverLockFreeWorker : public LockFreeWorker {
public:
    ReceiverLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown
    ) : LockFreeWorker(ctx, cfg, queue, false, shutdown) {};
    void run() override;
};

class DealerSenderLockFreeWorker : public SenderLockFreeWorker {
public:
    DealerSenderLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown
    ) : SenderLockFreeWorker(ctx, cfg, queue, shutdown, ZMQ_DEALER) {};
};

//class ReplySenderLockFreeWorker : public SenderLockFreeWorker {
//public:
//    ReplySenderLockFreeWorker()
//}

class ConnectionTesterWorker : public ThreadWorker {
public:
    ConnectionTesterWorker(void* ctx, const Config& cfg)
        : ThreadWorker(ctx, cfg) {};
    void run() override;
};

#endif
