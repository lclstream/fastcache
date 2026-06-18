#include <iostream>
#include <unistd.h>
#include "cacheserver.h"

// TODO: 
// - Add universal setup_socket func.
// - change to c++ vector handling
// - change pthread
// - Write some docs/readme
// - Add REQ
// - Add some more graceful way to exit atomic running?
// - Clean up config file and class variables (remove unused)
// - Structure overhaul, Worker class
// - modern ptrs

CacheServer::CacheServer(Config &config) 
    : cfg(config) {
    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.io_threads);

    std::cout << "Using " << cfg.io_threads << " zmq io threads." << std::endl;
    std::cout << "Trying to use " << cfg.helper_threads << " helper_threads." << std::endl;
    std::cout << "IN URL: " << cfg.inurl << " OUT URL: " << cfg.outurl << std::endl;
    std::cout << "Type: " << cfg.type << "." << std::endl;
    std::cout << "Verbose: " << cfg.verbose << "." << std::endl;

}

CacheServer::~CacheServer() {
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
        threads.emplace_back([&worker]() {
            worker->run();
        });
    }

    if (cfg.verbose && cfg.type == 4) {
        while(true) {
            int num = queue.read_available();
            if (num > 1) {
                std::cout << "Elements in queue: " << num << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
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
            for (int i=0; i<cfg.helper_threads+1; i++) {
                workerlist.push_back(std::make_unique<InprocWorker>(zmq_ctx, cfg, true, bindoutgoing));
            }
            break;
        }
        case 4:
            // lock free queue
            workerlist.push_back(std::make_unique<LockfreeWorker>(zmq_ctx, cfg, queue, false)); // receiver
            workerlist.push_back(std::make_unique<LockfreeWorker>(zmq_ctx, cfg, queue, true)); // sender
            break;
        case 5:
            // placeholder for request handler
            break;
        case 6: 
            workerlist.push_back(std::make_unique<ConnectionTesterWorker>(zmq_ctx, cfg));
            break;
        default:
            break;
    }

    return workerlist;
}



/* WIP - commented out for now
void* CacheServer::request_handler(void* selfptr) {

    // TODO Still work in progress
    CacheServer* self = static_cast<CacheServer*>(selfptr);

    void* response_socket = zmq_socket(self->zmq_ctx, ZMQ_REP);
    //void* response_socket = zmq_socket(self->zmq_ctx, ZMQ_PUSH);
    zmq_bind(response_socket, self->requrl.c_str());

    void* pull_from_inproc = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    zmq_connect(pull_from_inproc, self->workerurl.c_str());

    pthread_mutex_lock(&self->cache_lock);

    while(true) {
        std::cout << "Req thread -> going into mutex loop" << std::endl;
        while (!self->has_cache) {
            pthread_mutex_unlock(&self->cache_lock);
            usleep(100);
            pthread_mutex_lock(&self->cache_lock);
        }
        std::cout << "Req thread -> out of mutex" << std::endl;
        zmq_msg_t data;
        zmq_msg_init(&data);

        zmq_msg_move(&data, &self->message_cache);
        self->has_cache = false;

        pthread_mutex_unlock(&self->cache_lock);

        zmq_msg_t req;
        zmq_msg_init(&req);
        std::cout << "Req thread -> Receiving msg " << std::endl;
        if (zmq_msg_recv(&req, response_socket, 0) < 0) {
            zmq_msg_close(&req);
            break;
        }
        std::cout << "Req thread -> Received request" << std::endl;
        zmq_msg_close(&req);
        std::cout << "Req thread -> Sending data" << std::endl;

        zmq_msg_send(&data, response_socket, 0);
        std::cout << "Req thread -> Sent" << std::endl;
        //zmq_msg_close(&data);
    }

    zmq_close(response_socket);
    zmq_close(pull_from_inproc);
    return NULL;
}
*/
