#include <iostream>
#include <thread>
#include <sys/syscall.h>
#include <zmq.h>
#include "threadworker.h"

void* ThreadWorker::create_socket(void* ctx, const SocketConfig& socketcfg) {
    void* socket = zmq_socket(ctx, socketcfg.type);

    zmq_setsockopt(socket, socketcfg.type == ZMQ_PULL ? ZMQ_RCVHWM:ZMQ_SNDHWM, 
                   &socketcfg.hwm, sizeof(socketcfg.hwm));

    if (socketcfg.isbind) {
        zmq_bind(socket, socketcfg.url.c_str());
    } else {
        zmq_connect(socket, socketcfg.url.c_str());
    }

    return socket;

}

int ThreadWorker::send_receive(void* incoming, void* outgoing) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);

    int rc = zmq_msg_recv(&msg, incoming, 0);
    if (rc < 0) {
        zmq_msg_close(&msg);
        return -1;
    }

    rc = zmq_msg_send(&msg, outgoing, 0);
    if (rc < 0) {
        zmq_msg_close(&msg);
        return -1;
    }
    zmq_msg_close(&msg);

    return 0;
}

void ProxyWorker::run() {
    void *incoming = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
    void *outgoing = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});

    std::cout << "Starting simple forward with 1 thread. PID" << std::this_thread::get_id() << std::endl;

    int prob = 0;

    if (proxy) {
            zmq_proxy(incoming, outgoing, nullptr);
    } else {
        while (prob >= 0) {
            prob = send_receive(incoming, outgoing);
        }
    }

    zmq_close(incoming);
    zmq_close(outgoing);
}

void InprocWorker::run() {
    void *incoming;
    void *outgoing;

    if (!sender) {
        std::cout << "Starting Inproc forward. Receiver PID: " << std::this_thread::get_id() << std::endl;
        incoming = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
        outgoing = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.workerurl, true});
    } else {
        std::cout << "Starting Inproc forward. Sender PID: " << std::this_thread::get_id() << std::endl;
        incoming = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.workerurl, false});
        if (bindoutgoing) {
            outgoing = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});
        } else {
            outgoing = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, false});
        }
    }
    int prob = 0;
    while (prob >= 0) {
        prob = send_receive(incoming, outgoing);
    }
    zmq_close(incoming);
    zmq_close(outgoing);

}

void LockfreeWorker::run() {
    void* socket;
    pid_t tid = syscall(SYS_gettid);
    if (!sender) {
        std::cout << "Starting Lockfree forward. Receiver TID: " << tid << std::endl;
        socket = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
        while (1) {
            zmq_msg_t msg;
            zmq_msg_init(&msg);

            int rc = zmq_msg_recv(&msg, socket, 0);
            if (rc < 0) {
                zmq_msg_close(&msg);
                int err = zmq_errno();
                if (err == EAGAIN) {
                    continue;
                } else {
                    std::cerr << "Error, closing receiver thread." << std::endl;
                }
                break;
            }

            zmq_msg_t* qmsg = new zmq_msg_t();
            zmq_msg_init(qmsg);
            zmq_msg_move(qmsg, &msg); 
            while (!queue.push(qmsg)) {
                std::this_thread::yield();
            }
        }
    } else {
        std::cout << "Starting Lockfree forward. Sender TID: " << tid << std::endl;
        socket = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});
        while (1) {
            bool got_msg = false;
            zmq_msg_t* msg;
            if (queue.pop(msg)) {
                got_msg = true;
            }
            if (!got_msg) {
                std::this_thread::yield(); 
                continue;
            }
            int rc = zmq_msg_send(msg, socket, 0);
            zmq_msg_close(msg);
            delete msg;
            if (rc < 0) {
                break;
            }
        }
    }
    zmq_close(socket);
}

void ConnectionTesterWorker::run() {
    void* socket = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});

    const size_t size = 30ULL * 1024 * 1024;
    std::vector<uint8_t> buffer(size);
    for (size_t i=0; i<size; i++) {
        buffer[i] = static_cast<uint8_t>(i);
    }

    while(true) {
        zmq_msg_t msg;
        zmq_msg_init_data(
            &msg,
            buffer.data(),
            buffer.size(),
            nullptr,
            nullptr
        );
        zmq_msg_send(&msg, socket, 0);
        zmq_msg_close(&msg);
    }
}
