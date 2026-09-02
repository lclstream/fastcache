#include <iostream>
#include <unistd.h>
#include "cacheserver.h"


CacheServer::CacheServer(Config &config, std::atomic<bool>& shutdown_signal)
    : cfg(config), shutdown_signal(shutdown_signal) {
    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.zmq_io_threads);

    std::cout << "\n<<< Fastcache v0.3.0 >>> " << std::endl;
    std::cout << "\n----- General config ----- " << std::endl;
    std::cout << "zmq io threads:    " << cfg.zmq_io_threads << std::endl;
    std::cout << "Helper threads:    " << cfg.helper_threads << std::endl;
    std::cout << "IN URL:            " << cfg.inurl << std::endl;
    std::cout << "OUT URL:           " << cfg.outurl << std::endl;
    std::cout << "Type:              " << cfg.type << std::endl;
    if (cfg.timeout > 0) {
        std::cout << "Timeout:           " << cfg.timeout << " ms." << std::endl;
    }
    std::cout << "Verbose:           " << cfg.verbose << std::endl;

    std::cout << "\n----- Metrics config ----- " << std::endl;
    std::cout << "Interval:          " << cfg.metrics_interval << std::endl;
    std::cout << "Cache ID:          " << cfg.metrics_cache_id << std::endl;

    if (cfg.type == 7 || cfg.type == 8) {
        std::cout << "\n----- EJFat config -----" << std::endl;
        std::cout << "Use LB:            " << cfg.ejfat_useLB << std::endl;
        std::cout << "MTU:               " << cfg.ejfat_mtu << std::endl;
        std::cout << "Send buffer size:  " << cfg.ejfat_sndbufsize << std::endl;
        std::cout << "Rate (Gbps):       " << cfg.ejfat_rateGbps << std::endl;
        std::cout << "Send Sockets:      " << cfg.ejfat_numSendSockets << std::endl;
        std::cout << "Data sim threads:  " << cfg.dataSimulatorThreads << std::endl;
        std::cout << "Data ID:           " << cfg.ejfat_dataId << "\n" << std::endl;
    }
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
    workers = create(cfg, zmq_ctx);
    threads.reserve(workers.size());

    for (auto& worker: workers) {
        auto* workerptr = worker.get();
        threads.emplace_back([workerptr]() {
            workerptr->run();
        });
    }
    while (!shutdown_signal.load(std::memory_order_acquire)) {
        if (cfg.verbose && (cfg.type == 4 || cfg.type == 5 || cfg.type == 6 || cfg.type == 7)) {
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
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<SenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case 5: // lock free queue with push-pull in and dealer out
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<RouterSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case 6: //lock free queue with push-pull in and rep out
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<ReplySenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case 7: //lock free queue with push-pull in and EJFat out
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<EJFatSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case 8: //lock free queue with Simulated data and EJFat out
            // receiver:
            for (int i=0; i<cfg.dataSimulatorThreads; ++i) {
                workerlist.push_back(std::make_unique<QueueGeneratorLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            }
            // sender:
            workerlist.push_back(std::make_unique<EJFatSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        default:
            break;
    }

    return workerlist;
}
