#include <iostream>
#include <thread>
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

    std::cout << "Starting simple forward with 1 thread. TID" << std::this_thread::get_id() << std::endl;

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
    auto tid = std::this_thread::get_id();

    if (!sender) {
        std::cout << "Starting Inproc forward. Receiver TID: " << tid << std::endl;
        incoming = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
        outgoing = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.workerurl, true});
    } else {
        std::cout << "Starting Inproc forward. Sender TID: " << tid << std::endl;
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

std::string LockfreeWorker::create_metrics(uint64_t rc_count, uint64_t msg_count, uint64_t metrics_count) {
    auto timenow = std::chrono::system_clock::now();
    auto time_since_epoch = timenow.time_since_epoch();
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(time_since_epoch).count();
    json metrics_data = {
        {"type", sender ? "Sender" : "Receiver"},
        {"timestamp", timestamp},
        {"rc_count", rc_count},
        {"msg_count", msg_count},
        {"metrics_count", metrics_count}
    };
    return metrics_data.dump();
}

void LockfreeWorker::run() {
    void* socket;
    void* metrics_socket = nullptr;
    std::string metrics_path;
    auto tid = std::this_thread::get_id();
    uint64_t rc_count = 0;
    uint64_t msg_count = 0;
    uint64_t metrics_count = 0;
    if (!sender) {
        std::cout << "Starting Lockfree forward. Receiver TID: " << tid << std::endl;
        socket = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
        zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
        if (cfg.metrics) {
            int hwm = 10000;
            metrics_path = "/tmp/fastcache-metrics-receiver-" + std::to_string(cfg.cache_id);
            std::string receiver_metrics_url = "ipc://" + metrics_path;
            metrics_socket = create_socket(zmq_ctx, {ZMQ_PUB, hwm, receiver_metrics_url, true});
        }
        bool started_work = false;
        while (1) {
            zmq_msg_t msg;
            zmq_msg_init(&msg);

            int rc = zmq_msg_recv(&msg, socket, 0);
            if (rc < 0) {
                zmq_msg_close(&msg);
                int err = zmq_errno();
                if (err == EAGAIN) {
                    if (shutdown.load(std::memory_order_acquire)) break;
                    if (!started_work) continue;
                    if (timeout > 0) {
                        std::cout << "No messages for " << static_cast<double>(timeout)/1000 << " seconds. Exiting." << std::endl;
                    }
                } else {
                    std::cerr << "Error, " << zmq_strerror(err) << " closing threads." << std::endl;
                }
                shutdown.store(true, std::memory_order_release);
                break;
            } else if (cfg.metrics) {
                rc_count += rc;
                msg_count++;
                if (msg_count%cfg.metrics_interval == 0) {
                    std::string buffer = create_metrics(rc_count, msg_count, metrics_count);
                    if (metrics_socket && !buffer.empty()) {
                        zmq_send(metrics_socket, buffer.data(), buffer.size(), ZMQ_DONTWAIT);
                    }
                    rc_count = 0;
                    msg_count = 0;
                    metrics_count++;
                }
            }
            started_work = true;
            zmq_msg_t* qmsg = new zmq_msg_t();
            zmq_msg_init(qmsg);
            zmq_msg_move(qmsg, &msg); 
            while (!queue.push(qmsg)) {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            if (shutdown.load(std::memory_order_acquire)) {
                break;
            }
        }
    } else {
        std::cout << "Starting Lockfree forward. Sender TID: " << tid << std::endl;
        socket = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});
        if (cfg.metrics) {
            int hwm = 10000;
            metrics_path = "/tmp/fastcache-metrics-sender-" + std::to_string(cfg.cache_id);
            std::string sender_metrics_url = "ipc://" + metrics_path;
            metrics_socket = create_socket(zmq_ctx, {ZMQ_PUB, hwm, sender_metrics_url, true});
        }
        while (1) {
            zmq_msg_t* msg;
            if (queue.pop(msg)) {
                int rc = zmq_msg_send(msg, socket, 0);
                zmq_msg_close(msg);
                delete msg;
                if (rc < 0) {
                    break;
                } else if (cfg.metrics) {
                    rc_count += rc;
                    msg_count++;
                    if (msg_count%cfg.metrics_interval == 0) {
                        std::string buffer = create_metrics(rc_count, msg_count, metrics_count);
                        if (metrics_socket && !buffer.empty()) {
                            zmq_send(metrics_socket, buffer.data(), buffer.size(), ZMQ_DONTWAIT);
                        }
                        rc_count = 0;
                        msg_count = 0;
                        metrics_count++;
                    }
                }
            }
            if (shutdown.load(std::memory_order_acquire) && queue.read_available() == 0) {
                break;
            }
            std::this_thread::yield();
        }
    }
    zmq_close(socket);
    if (cfg.metrics) {
        std::cout << "Closing thread: " << tid << std::endl;
        zmq_close(metrics_socket);
        metrics_socket = nullptr;
        std::remove(metrics_path.c_str());
    }
}

void ConnectionTesterWorker::run() {

    //void* socket = create_socket(zmq_ctx, {testincoming ? ZMQ_PULL:ZMQ_PUSH,
    //        cfg.hwm, testincoming ? cfg.inurl:cfg.outurl, true});

    void* socket = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});

    const size_t size = 30ULL * 1024 * 1024;
    std::vector<uint8_t> buffer(size);
    for (size_t i=0; i<size; i++) {
        buffer[i] = static_cast<uint8_t>(i);
    }

    while (1) {
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
