#include <iostream>
#include <unistd.h>
#include "cacheserver.h"


CacheServer::CacheServer(Config &config, std::atomic<bool>& shutdown_signal)
    : cfg(config), shutdown_signal(shutdown_signal) {
    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.zmq_io_threads);

    std::cout << "\n<<< Fastcache v0.2.0 >>> " << std::endl;
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
        std::cout << "Send buffer size:  " << cfg.ejfat_socketbufsize << std::endl;
        std::cout << "Rate (Gbps):       " << cfg.ejfat_rateGbps << std::endl;
        std::cout << "Send Sockets:      " << cfg.ejfat_numSendSockets << std::endl;
        //std::cout << "Data sim threads:  " << cfg.dataSimulatorThreads << std::endl;
        std::cout << "Data ID:           " << cfg.ejfat_dataId << std::endl;
        std::cout << "Event Queue Size:  " << cfg.ejfat_queueSize << std::endl;
    }

    if (cfg.type == 9) {
        std::cout << "\n----- EJFat receiver config -----" << std::endl;
        std::cout << "Use LB:                 " << cfg.ejfat_useLB << std::endl;
        std::cout << "Send buffer size:       " << cfg.ejfat_socketbufsize << std::endl;
        std::cout << "Report stats to LB:     " << cfg.ejfat_reportstats << std::endl;
        std::cout << "Receiver threads:       " << cfg.ejfat_recvthreads << std::endl;
        //std::cout << "Reading threads:        " << cfg.ejfat_readthreads << std::endl;
        std::cout << "Timeout for reassembly: " << cfg.ejfat_eventtimeoutms << std::endl;
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
        case RunType::TestIn:
            workerlist.push_back(std::make_unique<SinkLockFreeWorker>(
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
        case RunType::ZMQPULLinEJFatOut:
            // receiver:
            workerlist.push_back(std::make_unique<ReceiverLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            // sender:
            workerlist.push_back(std::make_unique<EJFatSenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::TestInEJFatOut:
            // receiver:
            //for (int i=0; i<cfg.dataSimulatorThreads; ++i) {
            workerlist.push_back(std::make_unique<QueueGeneratorLockFreeWorker>(
                    zmq_ctx, cfg, queue, shutdown_signal));
            //}
            // sender:
            workerlist.push_back(std::make_unique<EJFatSenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case 9: {//lock free queue with EJFat receiver and ZMQ REPLY out
            // receiver:
            workerlist.push_back(std::make_unique<EJFatReceiverLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            //std::unique_ptr<EJFatReceiverLockFreeWorker> rsbmlr = 
            //    std::make_unique<EJFatReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal);
            //for (int i=0; i<cfg.ejfat_readthreads; ++i) {
            //
            //}
            // sender:
            workerlist.push_back(std::make_unique<ReplySenderLockFreeWorker>(
                zmq_ctx, cfg, queue, shutdown_signal));
            break;
        }
        default:
            break;
    }

    return workerlist;
}
