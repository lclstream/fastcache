#include <iostream>
#include <thread>
#include <unistd.h>
#include "cacheserver.h"

// TODO: 
// - Add universal setup_socket func.
// - change to c++ vector handling
// - change pthread
// - Write some docs/readme
// - Add REQ
// - Add some more graceful way to exit
// - Clean up config file and class variables (remove unused)

CacheServer::CacheServer(const Config &cfg) 
    : helper_threads(cfg.helper_threads),
      hwm(cfg.hwm),
      outurl(cfg.outurl),
      inurl(cfg.inurl),
      type(cfg.type),
      verbose(cfg.verbose) {

    if (type==5) {
        pthread_mutex_init(&cache_lock, NULL);
        has_cache = false;
    }

    zmq_ctx = zmq_ctx_new();
    zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, cfg.io_threads);

    std::cout << "Using " << cfg.io_threads << " zmq io threads." << std::endl;
    std::cout << "Trying to use " << cfg.helper_threads << " helper_threads." << std::endl;
    std::cout << "IN URL: " << inurl << " OUT URL: " << outurl << std::endl;
    std::cout << "Type: " << type << "." << std::endl;
    std::cout << "Verbose: " << verbose << "." << std::endl;
}

void CacheServer::Run() {

    switch (type) {
        case 0: // simple
        case 1: // proxy
            // 1 thread that is doing IN and OUT with proxy or simple
            num_workers = 1;
            workers.resize(num_workers);
            pthread_create(&workers[0], NULL, &CacheServer::proxy_worker, this);
            break;
        case 2: // bind inproc
        case 3: // connect inproc
            if (type == 2) helper_threads = 0; // Just to be sure
            num_workers = 2 + helper_threads;
            workers.resize(num_workers);
            pthread_create(&workers[0], NULL, &CacheServer::inproc_receiver_worker, this);
            for (auto i=1; i<num_workers; ++i) {
                pthread_create(&workers[i], NULL, &CacheServer::inproc_sending_worker, this);
            }
            break;
        case 4:
            // lock free queue
            if (helper_threads>1) {
                helper_threads = 1; // no need for more than 1 for now.
            }
            num_workers = 2 + helper_threads;
            workers.resize(num_workers);
            pthread_create(&workers[0], NULL, &CacheServer::lockfree_sender_worker, this);
            pthread_create(&workers[1], NULL, &CacheServer::lockfree_receiver_worker, this);
            if (num_workers>2) {
                std::cout << "Adding 1 extra receiver" << std::endl;
                pthread_create(&workers[2], NULL, &CacheServer::lockfree_receiver_worker2, this);
            }
            break;
        case 5:
            // placeholder for request handler
            break;
        case 6: 
            // test connection
            num_workers = 1;
            workers.resize(num_workers);
            pthread_create(&workers[0], NULL, &CacheServer::test_connection, this);
            break;
    }

    if (verbose && type == 4) {
        while(true) {
            int num = queue.read_available();
            int num2 = queue2.read_available();
            if (num >1 || num2 > 1) {
                std::cout << "Elements in queue1: " << num << "\tElements in queue2: " << num2 <<  std::endl;
            }
        }
    }
}

CacheServer::~CacheServer() {

    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i], NULL);
    }
    zmq_ctx_term(zmq_ctx);
}

int CacheServer::send_receive(void* incoming, void* outgoing) {

    zmq_msg_t msg;
    zmq_msg_init(&msg);

    int rc = zmq_msg_recv(&msg, incoming, 0);
    if (rc < 0) {
        zmq_msg_close(&msg);
        return -1;
    }
    // TEST DONTWAIT - makes a difference?
    rc = zmq_msg_send(&msg, outgoing, ZMQ_DONTWAIT);
    if (rc < 0) {
        zmq_msg_close(&msg);
        return -1;
    }
    zmq_msg_close(&msg);

    return 0;
}

void CacheServer::cpulock([[maybe_unused]] int num) {
#ifdef __linux__
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(num, &set);
    int res = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (res){
        std::cout << "Setting cpu affinity failed " << strerror(res) << std::endl;
    }
#else
    std::cout << "cpulock: CPU affinity not supported on this platform, skipping." << std::endl;
#endif
}

void* CacheServer::proxy_worker(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);

    void *incoming = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    void *outgoing = zmq_socket(self->zmq_ctx, ZMQ_PUSH);

    zmq_setsockopt(incoming, ZMQ_RCVHWM, &self->hwm, sizeof(self->hwm));
    zmq_setsockopt(outgoing, ZMQ_SNDHWM, &self->hwm, sizeof(self->hwm));

    zmq_bind(incoming, self->inurl.c_str());
    zmq_bind(outgoing, self->outurl.c_str());
    std::cout << "Starting simple forward with 1 thread. " << std::endl;

    int prob = 0;

    if (self->type == 1) {
            zmq_proxy(incoming, outgoing, nullptr);
    } else {
        while (prob >= 0) {
            prob = send_receive(incoming, outgoing);
        }
    }

    zmq_close(incoming);
    zmq_close(outgoing);
    return NULL;
}

void* CacheServer::inproc_sending_worker(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);

    std::cout << "Starting inproc forward." << std::endl;

    void* incoming = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    zmq_setsockopt(incoming, ZMQ_RCVHWM, &self->hwm, sizeof(self->hwm));
    zmq_connect(incoming, self->workerurl.c_str());

    void *outgoing = zmq_socket(self->zmq_ctx, ZMQ_PUSH);
    zmq_setsockopt(outgoing, ZMQ_SNDHWM, &self->hwm, sizeof(self->hwm));
    zmq_bind(outgoing, self->outurl.c_str());

    if (self->type == 3) {
        // fan out send connects
        zmq_connect(outgoing, self->outurl.c_str());
    } else { // 2
        zmq_bind(outgoing, self->outurl.c_str());
    }

    int prob = 0;
    while (prob >= 0) {
        prob = send_receive(incoming, outgoing);
    }
    zmq_close(incoming);
    zmq_close(outgoing);
    return NULL;
}

void* CacheServer::inproc_receiver_worker(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);

    void* receiver_socket = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    zmq_setsockopt(receiver_socket, ZMQ_RCVHWM, &self->hwm, sizeof(self->hwm));
    zmq_bind(receiver_socket, self->inurl.c_str());

    void* distributor_socket = zmq_socket(self->zmq_ctx, ZMQ_PUSH);
    zmq_setsockopt(distributor_socket, ZMQ_SNDHWM, &self->hwm, sizeof(self->hwm));
    zmq_bind(distributor_socket, self->workerurl.c_str() );

    int prob = 0;
    while(prob>=0) {
        prob = send_receive(receiver_socket, distributor_socket);
    }

    zmq_close(receiver_socket);
    zmq_close(distributor_socket);
    return NULL;

}

void* CacheServer::lockfree_receiver_worker(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);

    //self->cpulock(27);

    void* receiver_socket = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    zmq_setsockopt(receiver_socket, ZMQ_RCVHWM, &self->hwm, sizeof(self->hwm));
    zmq_bind(receiver_socket, self->inurl.c_str());

    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        int rc = zmq_msg_recv(&msg, receiver_socket, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            break;
        }
        zmq_msg_t* qmsg = new zmq_msg_t();
        zmq_msg_init(qmsg);
        zmq_msg_move(qmsg, &msg); 

        while (!self->queue.push(qmsg)) {
            std::this_thread::yield();
        }
        zmq_msg_close(&msg); 
    }
    return NULL;
}

void* CacheServer::lockfree_receiver_worker2(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);

    //self->cpulock(28);

    void* receiver_socket = zmq_socket(self->zmq_ctx, ZMQ_PULL);
    zmq_setsockopt(receiver_socket, ZMQ_RCVHWM, &self->hwm, sizeof(self->hwm));
    // Hardcoded URL because this entire function is just for testing and will be removed.
    zmq_bind(receiver_socket, "tcp://134.79.23.43:5002");

    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        int rc = zmq_msg_recv(&msg, receiver_socket, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            break;
        }
        zmq_msg_t* qmsg = new zmq_msg_t();
        zmq_msg_init(qmsg);
        zmq_msg_move(qmsg, &msg); 

        while (!self->queue2.push(qmsg)) {
            std::this_thread::yield();
        }
        zmq_msg_close(&msg); 
    }
    return NULL;
}

void* CacheServer::lockfree_sender_worker(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);
    //self->cpulock(29);

    void *outgoing = zmq_socket(self->zmq_ctx, ZMQ_PUSH);
    zmq_setsockopt(outgoing, ZMQ_SNDHWM, &self->hwm, sizeof(self->hwm));
    zmq_bind(outgoing, self->outurl.c_str());

    std::cout << "Starting lockfree queue workers. " << std::endl;

    bool flip = true;
    while (1) {
        bool got_msg = false;

        zmq_msg_t* msg;

        if (flip) {
            if (self->queue.pop(msg)) {
                got_msg = true;
            } else if (self->queue2.pop(msg)) {
                got_msg = true;
            }
        } else {
            if (self->queue2.pop(msg)) {
                got_msg = true;
            } else if (self->queue.pop(msg)) {
                got_msg = true;
            }
        }

        flip = !flip;

        if (!got_msg) {
            std::this_thread::yield(); 
            continue;
        }
        // TODO Remove the ZMQ_DONTWAIT flag, does not make a diff.
        int rc = zmq_msg_send(msg, outgoing, ZMQ_DONTWAIT);
        zmq_msg_close(msg);
        if (rc < 0) {
            break;
        }
    }
    zmq_close(outgoing);
    return NULL;
}


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

void* CacheServer::test_connection(void* selfptr) {

    CacheServer* self = static_cast<CacheServer*>(selfptr);

    void *outgoing = zmq_socket(self->zmq_ctx, ZMQ_PUSH);
    zmq_setsockopt(outgoing, ZMQ_SNDHWM, &self->hwm, sizeof(self->hwm));
    zmq_bind(outgoing, self->outurl.c_str());

    const size_t SIZE = 30ULL * 1024 * 1024;
    std::vector<uint8_t> buffer(SIZE);

    for (size_t i = 0; i < SIZE; i++) {
        buffer[i] = static_cast<uint8_t>(i);
    }
    while(true) {
        zmq_msg_t msg2;
        zmq_msg_init_data(
            &msg2,
            buffer.data(),
            buffer.size(),
            nullptr,
            nullptr
        );
        zmq_msg_send(&msg2, outgoing, 0);
        zmq_msg_close(&msg2);
    }
    return NULL;
}
