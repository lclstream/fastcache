#ifndef THREADWORKER_H
#define THREADWORKER_H
#pragma once

#include <string>
#include "config.h"
#include <boost/lockfree/spsc_queue.hpp>
#include <thread>
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

enum class Action { Continue, Break, Resume };

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
        std::atomic<bool>& shutdown_signal
    ) : ThreadWorker(ctx, cfg), shutdown_signal(shutdown_signal),
                                sender(sender),
                                queue(queue),
                                timeout(cfg.timeout) {};
    //void run() override;
protected:
    std::atomic<bool>& shutdown_signal;
    bool sender;
    MessageQueue& queue;
    int timeout;
protected:
    std::string create_metrics(uint64_t rc_count, uint64_t msg_count, uint64_t metrics_count);
    void send_metrics(MetricsData& metrics, int rc, void* metrics_socket);
    void* create_metrics_socket(std::string& metrics_path);
    void cleanup_metrics(std::thread::id tid, void* metrics_socket, std::string& metrics_path);
};

class QueueGeneratorLockFreeWorker : public LockFreeWorker {
public:
    QueueGeneratorLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown_signal
    ) : LockFreeWorker(ctx, cfg, queue, false, shutdown_signal) {};
    void run() override;
};

class ReceiverLockFreeWorker : public LockFreeWorker {
public:
    ReceiverLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown_signal
    ) : LockFreeWorker(ctx, cfg, queue, false, shutdown_signal) {};
    void run() override;
};

class SenderLockFreeWorker : public LockFreeWorker {
public:
    SenderLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown_signal,
        int socket_type = ZMQ_PUSH
    ) : LockFreeWorker(ctx, cfg, queue, true, shutdown_signal), socket_type(socket_type) {};
    void run() override;
    virtual Action receive(void* socket);
    virtual int send(void* socket, zmq_msg_t* msg);
protected:
    static void free_msg(zmq_msg_t* msg);
private:
    int socket_type;
};

class RouterSenderLockFreeWorker : public SenderLockFreeWorker {
public:
    RouterSenderLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown_signal
    ) : SenderLockFreeWorker(ctx, cfg, queue, shutdown_signal, ZMQ_ROUTER) {};
    ~RouterSenderLockFreeWorker() {
        zmq_msg_close(&id); 
    }
    Action receive(void* socket) override;
    int send(void* socket, zmq_msg_t* msg) override;
public:
    zmq_msg_t id;
};

class ReplySenderLockFreeWorker : public SenderLockFreeWorker {
public:
    ReplySenderLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown_signal
    ) : SenderLockFreeWorker(ctx, cfg, queue, shutdown_signal, ZMQ_REP) {};
    Action receive(void* socket) override;
};

class EJFatSenderLockFreeWorker : public SenderLockFreeWorker {
public:
    EJFatSenderLockFreeWorker(
        void* ctx,
        const Config& cfg,
        MessageQueue& queue,
        std::atomic<bool>& shutdown_signal
    ) : SenderLockFreeWorker(ctx, cfg, queue, shutdown_signal, -1),
        segmenter_flags([&cfg] {
            e2sar::Segmenter::SegmenterFlags flags;
            flags.useCP = cfg.ejfat_useLB;
            flags.mtu = cfg.ejfat_mtu;
            flags.sndSocketBufSize = cfg.ejfat_sndbufsize;
            flags.rateGbps = cfg.ejfat_rateGbps;
            flags.numSendSockets = cfg.ejfat_numSendSockets;
            flags.eventQueueSize = cfg.ejfat_queueSize;
            return flags;
        }()),
        uri(cfg.ejfat_uri, e2sar::EjfatURI::TokenType::instance, false),
        segmenter(new e2sar::Segmenter(uri, cfg.ejfat_dataId, 0x00000001, segmenter_flags)) {
            //std::vector<std::string> optimizations{"sendmmsg"};
            //auto ropt = e2sar::Optimizations::select(optimizations);
            //if (ropt.has_error()) {
            //    std::cerr << "Error, failed to set optimization: " << ropt.error().message() << std::endl;
            //    std::exit(1);
            //}
            std::cout << cfg.ejfat_uri << std::endl;
            auto open_result = segmenter->openAndStart();
            if (open_result.has_error()) {
               std::cerr << "Error, failed to start segmenter: " << open_result.error().message() << std::endl;;
               std::exit(1);
            }
            std::cout << "E2SAR Segmenter started successfully" << std::endl;
        };
    int send(void* socket, zmq_msg_t* msg) override;
protected:
    static void free_msg(boost::any arg);
private:
    e2sar::Segmenter::SegmenterFlags segmenter_flags;
    e2sar::EjfatURI uri;
    e2sar::Segmenter* segmenter;
    //std::unique_ptr<e2sar::Segmenter> segmenter;
};

#endif
