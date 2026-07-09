#include <iostream>
#include <unistd.h>
#include "cacheserver.h"


CacheServer::CacheServer(Config &config, std::atomic<bool>& shutdown)
    : cfg(config), shutdown(shutdown) {
    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.io_threads);

    std::cout << "Using " << cfg.io_threads << " zmq io threads." << std::endl;
    std::cout << "Trying to use " << cfg.helper_threads << " helper_threads." << std::endl;
    std::cout << "IN URL: " << cfg.inurl << " OUT URL: " << cfg.outurl << std::endl;
    std::cout << "Type: " << cfg.type << "." << std::endl;
    if (cfg.timeout > 0) {
        std::cout << "Timeout: " << cfg.timeout << " ms." << std::endl;
    }
    std::cout << "Verbose: " << cfg.verbose << "." << std::endl;
    std::cout << "Metrics: " << cfg.metrics << "." << std::endl;
}

CacheServer::~CacheServer() {
    std::cout << "Shutting down all threads. " << std::endl;
    shutdown.store(true, std::memory_order_release);
    for (auto& thread: threads) {
        if (thread.joinable())
            thread.join();
    }
    if (zmq_ctx) {
        zmq_ctx_term(zmq_ctx);
    }
}

void CacheServer::Run() {
    workers = create(cfg, zmq_ctx);
    threads.reserve(workers.size());

    for (auto& worker: workers) {
        auto* workerptr = worker.get();
        threads.emplace_back([workerptr]() {
            workerptr->run();
        });
    }
    while (!shutdown.load(std::memory_order_acquire)) {
        if (cfg.verbose && cfg.type == 4) {
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
        case 0: // simple
        case 1: // proxy
            // 1 thread that is doing IN and OUT with proxy or simple
            workerlist.push_back(std::make_unique<ProxyWorker>(zmq_ctx, cfg, cfg.type));
            break;
        case 2: // bind inproc
        case 3: {// connect inproc
            bool bindoutgoing = false;
            if (cfg.type == 2) {
                cfg.helper_threads = 0; // Just to be sure (w bind out no extra workers)
                bindoutgoing = true;
            }
            // receiver worker first! Inproc bind has to happen first
            workerlist.push_back(std::make_unique<InprocWorker>(zmq_ctx, cfg, false, bindoutgoing));
            for (ssize_t i=0; i<cfg.helper_threads+1; i++) {
                workerlist.push_back(std::make_unique<InprocWorker>(zmq_ctx, cfg, true, bindoutgoing));
            }
            break;
        }
        case 4: // lock free queue
            // receiver:
            workerlist.push_back(std::make_unique<LockfreeWorker>(zmq_ctx, cfg, queue, false, shutdown));
            // sender
            workerlist.push_back(std::make_unique<LockfreeWorker>(zmq_ctx, cfg, queue, true, shutdown));
            break;
        case 5:
            // placeholder for request handler
            break;
        case 6:  // test connection
            workerlist.push_back(std::make_unique<ConnectionTesterWorker>(zmq_ctx, cfg));
            break;
        default:
            break;
    }

    return workerlist;
}
