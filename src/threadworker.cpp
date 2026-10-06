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

void* LockFreeWorker::create_metrics_socket(std::string& metrics_path) {
    int hwm = 10000;
    std::string role = sender ? "sender" : "receiver";
    metrics_path = "/tmp/fastcache-metrics-" + role + "-" + std::to_string(cfg.cache_id);
    std::string sender_metrics_url = "ipc://" + metrics_path;
    return create_socket(zmq_ctx, {ZMQ_PUB, hwm, sender_metrics_url, true});
}

std::string LockFreeWorker::create_metrics(uint64_t rc_count, uint64_t msg_count, uint64_t metrics_count) {
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

void LockFreeWorker::send_metrics(MetricsData& metrics, int rc, void* metrics_socket) {
    metrics.rc_count += rc;
    metrics.msg_count++;
    if (metrics.msg_count%cfg.metrics_interval == 0) {
        std::string buffer = create_metrics(metrics.rc_count, metrics.msg_count, metrics.metrics_count);
        if (metrics_socket && !buffer.empty()) {
            zmq_send(metrics_socket, buffer.data(), buffer.size(), ZMQ_DONTWAIT);
        }
        metrics.rc_count = 0;
        metrics.msg_count = 0;
        metrics.metrics_count++;
    }
}

void LockFreeWorker::cleanup_metrics(std::thread::id tid, void* metrics_socket, std::string& metrics_path) {
    std::cout << "Closing thread: " << tid << std::endl;
    zmq_close(metrics_socket);
    metrics_socket = nullptr;
    std::remove(metrics_path.c_str());
}

Action SenderLockFreeWorker::receive(void*) {
    return Action::Resume;
}

Action ReplySenderLockFreeWorker::receive(void* socket) {
    zmq_msg_t request;
    zmq_msg_init(&request);
    int rc = zmq_msg_recv(&request, socket, 0);
    if (rc < 0) {
        if (errno == EAGAIN) {
            if (shutdown.load(std::memory_order_acquire))
                return Action::Break;
            return Action::Continue;
        }
        return Action::Break;
    }
    zmq_msg_close(&request);
    return Action::Resume;
}

Action RouterSenderLockFreeWorker::receive(void* socket) {
    zmq_msg_close(&id);
    zmq_msg_init(&id);
    int rc = zmq_msg_recv(&id, socket, 0);
    if (rc < 0) {
        if (errno == EAGAIN) {
            if (shutdown.load(std::memory_order_acquire)) {
                return Action::Break;
            }
            return Action::Continue;
        }
        return Action::Break;
    }
    zmq_msg_t empty;
    zmq_msg_t request;
    zmq_msg_init(&empty);
    zmq_msg_init(&request);
    zmq_msg_recv(&empty, socket, 0);
    zmq_msg_recv(&request, socket, 0);
    zmq_msg_close(&empty);
    zmq_msg_close(&request);
    return Action::Resume;
}

int SenderLockFreeWorker::send(void* socket, zmq_msg_t* msg) {
    return zmq_msg_send(msg, socket, 0);
}

int RouterSenderLockFreeWorker::send(void* socket, zmq_msg_t* msg) {
    //zmq_msg_t id_copy;
    //zmq_msg_init(&id_copy);
    //zmq_msg_copy(&id_copy, &id);
    //int rc = zmq_msg_send(&id_copy, socket, ZMQ_SNDMORE);
    int rc = zmq_msg_send(&id, socket, ZMQ_SNDMORE);
    if (rc < 0) {
        //zmq_msg_close(&id_copy);
        zmq_msg_close(&id);
        return -1;
    }
    zmq_msg_t empty;
    zmq_msg_init(&empty);
    rc = zmq_msg_send(&empty, socket, ZMQ_SNDMORE);
    if (rc < 0) {
        zmq_msg_close(&empty);
        return -1;
    }
    return zmq_msg_send(msg, socket, 0);
}

void SenderLockFreeWorker::run() {
    void* socket;
    void* metrics_socket = nullptr;
    std::string metrics_path;
    MetricsData metrics_data;
    auto tid = std::this_thread::get_id();

    std::cout << "Starting Lockfree forward. Sender TID: " << tid << std::endl;
    socket = create_socket(zmq_ctx, {socket_type, cfg.hwm, cfg.outurl, true});
    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    if (cfg.metrics) {
        metrics_socket = create_metrics_socket(metrics_path);
    }
    while (1) {
        if (socket_type == ZMQ_REP || socket_type == ZMQ_ROUTER) {
            Action ret = receive(socket);
            if (ret == Action::Break) break;
            if (ret == Action::Continue) continue;
        }

        zmq_msg_t* msg = nullptr;
        while (!queue.pop(msg)) {
            if (shutdown.load(std::memory_order_acquire) && queue.read_available() == 0) break;
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        if (shutdown.load(std::memory_order_acquire) && msg == nullptr) break;
        int rc = send(socket, msg);
        zmq_msg_close(msg);
        delete msg;
        if (rc < 0) break;
        if (cfg.metrics) {
            send_metrics(metrics_data, rc, metrics_socket);
        }
    }
    zmq_close(socket);
    if (cfg.metrics) {
        cleanup_metrics(tid, metrics_socket, metrics_path);
    }
}

void ReceiverLockFreeWorker::run() {
    void* socket;
    void* monitor_socket;
    void* metrics_socket = nullptr;
    std::string metrics_path;
    MetricsData metrics_data;
    auto tid = std::this_thread::get_id();

    std::cout << "Starting Lockfree forward. Receiver TID: " << tid << std::endl;
    socket = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});

    zmq_socket_monitor(socket, "inproc://monitor-recv",
                       ZMQ_EVENT_ACCEPTED | ZMQ_EVENT_DISCONNECTED);
    monitor_socket = zmq_socket(zmq_ctx, ZMQ_PAIR);
    zmq_connect(monitor_socket, "inproc://monitor-recv");

    if (cfg.metrics) {
        metrics_socket = create_metrics_socket(metrics_path);
    }

    int prod_seen = 0;
    int producers = 0;

    // Shutdown chain:
    // 1. Last expected producer disconnects → shutdown=true → receiver exits
    // 2. Sender drains queue → exits → closes PUSH socket
    // 3. Downstream consumer sees PUSH socket close → exits
    zmq_pollitem_t items[2] = {
        {socket,         0, ZMQ_POLLIN, 0},
        {monitor_socket, 0, ZMQ_POLLIN, 0},
    };

    while (1) {
        if (shutdown.load(std::memory_order_acquire)) break;

        int rc = zmq_poll(items, 2, timeout);
        if (rc < 0) {
            int err = zmq_errno();
            if (err == EINTR) {
                if (shutdown.load(std::memory_order_acquire)) break;
                continue;
            }
            std::cerr << "Error, " << zmq_strerror(err) << " closing threads." << std::endl;
            shutdown.store(true, std::memory_order_release);
            break;
        }

        if (rc == 0) {
            // Timeout fallback: if producers were seen but went quiet, treat as done.
            if (timeout > 0 && prod_seen > 0) {
                std::cout << "No messages for " << static_cast<double>(timeout)/1000
                          << " seconds. Exiting." << std::endl;
                shutdown.store(true, std::memory_order_release);
                break;
            }
            continue;
        }

        // Monitor event: producer connected or disconnected.
        if (items[1].revents & ZMQ_POLLIN) {
            zmq_msg_t event_msg;
            zmq_msg_init(&event_msg);
            zmq_msg_recv(&event_msg, monitor_socket, 0);
            uint16_t event = *(uint16_t *)zmq_msg_data(&event_msg);
            zmq_msg_close(&event_msg);
            zmq_msg_t addr_msg;
            zmq_msg_init(&addr_msg);
            zmq_msg_recv(&addr_msg, monitor_socket, 0);
            zmq_msg_close(&addr_msg);

            if (event == ZMQ_EVENT_ACCEPTED) {
                producers++;
                prod_seen++;
                std::cout << "Producer connected ("
                          << producers << " active, " << prod_seen << " seen)." << std::endl;
            } else if (event == ZMQ_EVENT_DISCONNECTED) {
                producers--;
                std::cout << "Producer disconnected ("
                          << producers << " active, " << prod_seen << " seen)." << std::endl;
                // Step 1 of shutdown chain: all expected producers have connected
                // and are now gone.
                if (producers <= 0 && prod_seen >= cfg.expected_producers) {
                    shutdown.store(true, std::memory_order_release);
                    break;
                }
            }
        }

        // Data available: receive and enqueue.
        if (items[0].revents & ZMQ_POLLIN) {
            zmq_msg_t msg;
            zmq_msg_init(&msg);
            int rc = zmq_msg_recv(&msg, socket, ZMQ_DONTWAIT);
            if (rc < 0) {
                zmq_msg_close(&msg);
                int err = zmq_errno();
                if (err == EAGAIN) continue;
                std::cerr << "Error, " << zmq_strerror(err) << " closing threads." << std::endl;
                shutdown.store(true, std::memory_order_release);
                break;
            }
            if (cfg.metrics) {
                send_metrics(metrics_data, rc, metrics_socket);
            }
            zmq_msg_t* qmsg = new zmq_msg_t();
            zmq_msg_init(qmsg);
            zmq_msg_move(qmsg, &msg);
            while (!queue.push(qmsg)) {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            if (shutdown.load(std::memory_order_acquire)) break;
        }
    }

    zmq_close(socket);
    zmq_close(monitor_socket);
    if (cfg.metrics) {
        cleanup_metrics(tid, metrics_socket, metrics_path);
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
