#include <iostream>
#include <thread>
#include "cacheserver.h"


CacheServer::CacheServer(const Config &cfg) 
    : num_workers(cfg.num_workers),
      hwm(cfg.hwm),
      outurl(cfg.outurl) {

    //zmq::context_t zmq_ctx;
    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.io_threads);

    //std::cout << "Using " << cfg.io_threads << " io threads." << std::endl;

    // socket that receives from mpi ranks
    receiver_socket = zmq_socket(zmq_ctx, ZMQ_PULL);
    //std::cout << "Using " << cfg.hwm << " hwm." << std::endl;
    zmq_setsockopt(receiver_socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    zmq_bind(receiver_socket, cfg.inurl.c_str());

    // socket that distributes work to the worker threads
    distributor_socket = zmq_socket(zmq_ctx, ZMQ_PUSH);
    zmq_setsockopt(distributor_socket, ZMQ_SNDHWM, &hwm, sizeof(hwm));
    zmq_bind(distributor_socket, workerurl.c_str() );


    workers.resize(num_workers);
    for (int i = 0; i < num_workers; i++) {
        pthread_create(&workers[i], NULL, &CacheServer::proxy_worker, this);
    }
    std::cout << "Started cache with " << num_workers << "threads." << std::endl;
}

void CacheServer::Run() {

    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        if (zmq_msg_recv(&msg, receiver_socket, 0) < 0) {
            zmq_msg_close(&msg);
            break;
        }
        if (zmq_msg_send(&msg, distributor_socket, 0) < 0) {
            zmq_msg_close(&msg);
            break;
        }

        zmq_msg_close(&msg);
    }
}

CacheServer::~CacheServer() {

    zmq_close(receiver_socket);
    zmq_close(distributor_socket);
    zmq_ctx_term(zmq_ctx);

    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i], NULL);
    }
}

void* CacheServer::proxy_worker(void *cs) {

    CacheServer* self = static_cast<CacheServer*>(cs);

    void *incoming = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    void *outgoing = zmq_socket(self->zmq_ctx, ZMQ_PUSH);

    zmq_setsockopt(incoming, ZMQ_RCVHWM, &self->hwm, sizeof(self->hwm));
    zmq_setsockopt(outgoing, ZMQ_SNDHWM, &self->hwm, sizeof(self->hwm));

    zmq_connect(incoming, self->workerurl.c_str());
    zmq_connect(outgoing, self->outurl.c_str());

    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        int rc = zmq_msg_recv(&msg, incoming, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            break;
        }
        rc = zmq_msg_send(&msg, outgoing, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            break;
        }
        zmq_msg_close(&msg);
    }
    zmq_close(incoming);
    zmq_close(outgoing);
    return NULL;

}
