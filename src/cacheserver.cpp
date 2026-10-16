#include <iostream>
#include <unistd.h>
#include "cacheserver.h"


CacheServer::CacheServer(Config &config, std::atomic<bool>& shutdown_signal)
    : cfg(config), shutdown_signal(shutdown_signal) {
    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.io_threads);

    std::cout << "Using " << cfg.io_threads << " zmq io threads." << std::endl;
    std::cout << "Trying to use " << cfg.helper_threads << " helper_threads." << std::endl;
    std::cout << "IN URL: " << cfg.inurl << " OUT URL: " << cfg.outurl << std::endl;
    std::cout << "Type: " << static_cast<int>(cfg.type) << "." << std::endl;
    if (cfg.timeout > 0) {
        std::cout << "Timeout: " << cfg.timeout << " ms." << std::endl;
    }
    std::cout << "Verbose: " << cfg.verbose << "." << std::endl;
    std::cout << "Metrics interval: " << cfg.metrics_interval << "." << std::endl;
    std::cout << "Cache ID: " << cfg.cache_id << "." << std::endl;
}

CacheServer::~CacheServer() {
    std::cout << "Shutting down all threads. " << std::endl;
    shutdown_signal.store(true, std::memory_order_release);
    for (auto& thread: threads) {
        if (thread.joinable())
            thread.join();
    }
    if (zmq_ctx) {
        zmq_ctx_term(zmq_ctx);
    }
}

void CacheServer::run() {
    // Main thread managing workers and reporting queue depth for lock-free modes.
    workers = create(cfg, zmq_ctx);
    threads.reserve(workers.size());

    for (auto& worker: workers) {
        auto* workerptr = worker.get();
        threads.emplace_back([workerptr]() {
            workerptr->run();
        });
    }
    while (!shutdown_signal.load(std::memory_order_acquire)) {
        // Set queue types here
        bool usesQueue = (cfg.type == RunType::LockFreePush ||
                         cfg.type == RunType::LockFreeDealer ||
                         cfg.type == RunType::LockFreeReply ||
                         cfg.type == RunType::TestOut);
        if (cfg.verbose && usesQueue) {
            int num = queue.read_available();
            if (num > 1) {
                std::cout << "Elements in queue: " << num << std::endl;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

std::vector<std::unique_ptr<ThreadWorker>> CacheServer::create(Config& cfg, void* zmq_ctx) {
    std::vector<std::unique_ptr<ThreadWorker>> workerlist;
    switch (cfg.type) {
        case RunType::Simple:
        case RunType::Proxy:
            // 1 thread that is doing IN and OUT with proxy or simple
            workerlist.push_back(std::make_unique<ProxyWorker>(
                zmq_ctx, cfg, static_cast<int>(cfg.type)));
            break;
        case RunType::BindInproc:
        case RunType::ConnectInproc: {
            bool bindoutgoing = false;
            if (static_cast<int>(cfg.type) == 2) {
                cfg.helper_threads = 0; // Just to be sure (w bind out no extra workers)
                bindoutgoing = true;
            }
            // receiver worker first! Inproc bind has to happen first
            workerlist.push_back(std::make_unique<InprocWorker>(
                zmq_ctx, cfg, false, bindoutgoing));
            for (ssize_t i=0; i<cfg.helper_threads+1; i++) {
                workerlist.push_back(std::make_unique<InprocWorker>(
                    zmq_ctx, cfg, true, bindoutgoing));
            }
            break;
        }
        case RunType::LockFreePush:
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<SenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::LockFreeDealer:
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<RouterSenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::LockFreeReply:
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<ReplySenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::TestOut:
            // receiver:
            for (int i=0; i<cfg.datasim_threads; ++i) {
                workerlist.push_back(std::make_unique<QueueGeneratorLockFreeWorker>(
                    zmq_ctx, cfg, queue, shutdown_signal));
            }
            // sender:
            workerlist.push_back(std::make_unique<SenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::TestIn:
            workerlist.push_back(std::make_unique<SinkLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
        default:
            break;
    }

    return workerlist;
}
