#include <iostream>
#include <thread>
#include <zmq.h>
#include "threadworker.h"

e2sar::Reassembler* WorkerFactory::addEJFatReceiver(
    std::vector<std::unique_ptr<ThreadWorker>>& workerlist,
    void* zmq_ctx,
    const Config& cfg,
    MessageQueue& queue,
    std::atomic<bool>& shutdown_signal)
{
    auto receiver = std::make_unique<EJFatReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal);
    auto* reassembler = receiver->getReassembler();
    workerlist.push_back(std::move(receiver));
    return reassembler;
}

std::vector<std::unique_ptr<ThreadWorker>> WorkerFactory::createWorkers(
    void* zmq_ctx,
    const Config& cfg,
    MessageQueue& queue,
    std::atomic<bool>& shutdown_signal) {

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
            workerlist.push_back(
                std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            workerlist.push_back(
                std::make_unique<SenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::LockFreeDealer:
            workerlist.push_back(
                std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            workerlist.push_back(
                std::make_unique<RouterSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::LockFreeReply:
            workerlist.push_back(
                std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            workerlist.push_back(
                std::make_unique<ReplySenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::TestIn:
            workerlist.push_back(
                std::make_unique<SinkLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::TestOut:
            //for (int i=0; i<cfg.datasim_threads; ++i) {
            workerlist.push_back(
                std::make_unique<QueueGeneratorLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            //}
            workerlist.push_back(
                std::make_unique<SenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::ZMQPULLinEJFatOut:
            workerlist.push_back(
                std::make_unique<ReceiverLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            workerlist.push_back(
                std::make_unique<EJFatSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::TestInEJFatOut:
            //for (int i=0; i<cfg.dataSimulatorThreads; ++i) {
            workerlist.push_back(
                std::make_unique<QueueGeneratorLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            //}
            workerlist.push_back(
                std::make_unique<EJFatSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            break;
        case RunType::EJFatInZMQPUSHOut: {
            e2sar::Reassembler* reassembler = addEJFatReceiver(
                workerlist, zmq_ctx, cfg, queue, shutdown_signal);
            workerlist.push_back(
                std::make_unique<SenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            if (cfg.verbose) {
                workerlist.push_back(
                    std::make_unique<EJFatReassemblerStatWorker>(zmq_ctx, cfg, reassembler, shutdown_signal));
            }
            break;
        }
        case RunType::EJFatInZMQREPOut: {
            e2sar::Reassembler* reassembler = addEJFatReceiver(
                workerlist, zmq_ctx, cfg, queue, shutdown_signal);
            workerlist.push_back(
                std::make_unique<ReplySenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            if (cfg.verbose) {
                workerlist.push_back(
                    std::make_unique<EJFatReassemblerStatWorker>(zmq_ctx, cfg, reassembler, shutdown_signal));
            }
            break;
        }
        case RunType::EJFatInZMQROUTEROut: {
            e2sar::Reassembler* reassembler = addEJFatReceiver(
                workerlist, zmq_ctx, cfg, queue, shutdown_signal);
            workerlist.push_back(
                std::make_unique<RouterSenderLockFreeWorker>(zmq_ctx, cfg, queue, shutdown_signal));
            if (cfg.verbose) {
                workerlist.push_back(
                    std::make_unique<EJFatReassemblerStatWorker>(zmq_ctx, cfg, reassembler, shutdown_signal));
            }
            break;
        }
        default:
            std::cout << "Error: Unknown RunType" << std::endl;
            std::exit(1);
    }
    return workerlist;
}

void* ThreadWorker::create_socket(void* ctx, const SocketConfig& socketcfg) {
    void* socket = zmq_socket(ctx, socketcfg.type);

    zmq_setsockopt(socket, socketcfg.type == ZMQ_PULL ? ZMQ_RCVHWM:ZMQ_SNDHWM,
                   &socketcfg.hwm, sizeof(socketcfg.hwm));
    zmq_setsockopt(socket, ZMQ_LINGER, &cfg.timeout, sizeof(cfg.timeout));

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
    void* incoming = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
    void* outgoing = create_socket(zmq_ctx, {ZMQ_PUSH, cfg.hwm, cfg.outurl, true});

    std::cout << "Starting simple forward with 1 thread. TID"
              << std::this_thread::get_id()
              << std::endl;

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
    void* incoming;
    void* outgoing;
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
    metrics_path = "/tmp/fastcache-metrics-" + role + "-" + std::to_string(cfg.metrics_cache_id);
    std::string sender_metrics_url = "ipc://" + metrics_path;
    return create_socket(zmq_ctx, {ZMQ_PUB, hwm, sender_metrics_url, true});
}

void* LockFreeWorker::create_metrics_socket() {
    int hwm = 10000;
    return create_socket(zmq_ctx, {ZMQ_PUSH, hwm, cfg.metrics_url, true});
}

std::string LockFreeWorker::create_metrics(uint64_t rc_count, uint64_t msg_count,
                                           uint64_t metrics_count) {
    auto timenow = std::chrono::system_clock::now();
    auto time_since_epoch = timenow.time_since_epoch();
    uint64_t timestamp =
                std::chrono::duration_cast<std::chrono::milliseconds>(time_since_epoch).count();
    nlohmann::json metrics_data = {
        {"type", sender ? "Sender" : "Receiver"},
        {"timestamp", timestamp},
        {"rc_count", rc_count},
        {"msg_count", msg_count},
        {"metrics_count", metrics_count}
    };
    return metrics_data.dump();
}

void LockFreeWorker::send_metrics(MetricsData& metrics, int rc, void* metrics_socket) {
    // Updates metrics counters, create metrics data and send them via zmq to metrics socket.
    metrics.rc_count += rc;
    metrics.msg_count++;
    if (metrics.msg_count%cfg.metrics_interval == 0) {
        std::string buffer = create_metrics(metrics.rc_count, metrics.msg_count,
                                            metrics.metrics_count);
        if (metrics_socket && !buffer.empty()) {
            zmq_send(metrics_socket, buffer.data(), buffer.size(), ZMQ_DONTWAIT);
        }
        metrics.rc_count = 0;
        metrics.msg_count = 0;
        metrics.metrics_count++;
    }
}

void LockFreeWorker::cleanup_metrics(std::thread::id tid, void* metrics_socket,
                                     std::string& metrics_path) {
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
            if (shutdown_signal.load(std::memory_order_acquire))
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
            if (shutdown_signal.load(std::memory_order_acquire)) {
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

void SenderLockFreeWorker::free_msg(zmq_msg_t* msg) {
    if (msg) {
        zmq_msg_close(msg);
        delete msg;
    }
}

int SenderLockFreeWorker::send(void* socket, zmq_msg_t* msg) {
    int rc = zmq_msg_send(msg, socket, 0);
    free_msg(msg);
    return rc;
}

int RouterSenderLockFreeWorker::send(void* socket, zmq_msg_t* msg) {
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
    rc = zmq_msg_send(msg, socket, 0);
    free_msg(msg);
    return rc;
}

void EJFatSenderLockFreeWorker::free_msg(boost::any arg) {
    zmq_msg_t* msg = boost::any_cast<zmq_msg_t*>(arg);
    SenderLockFreeWorker::free_msg(msg);
}

int EJFatSenderLockFreeWorker::send(void* /*socket*/, zmq_msg_t* msg) {
    uint8_t* event_data = reinterpret_cast<uint8_t*>(zmq_msg_data(msg));
    std::size_t event_len = zmq_msg_size(msg);
    while (1) {
        auto send_result = segmenter->addToSendQueue(event_data, event_len, 0, 0, 0, 
                                                     EJFatSenderLockFreeWorker::free_msg, msg);
        if (!send_result.has_error()) {
            return static_cast<int>(event_len);
        } else {
            if (shutdown_signal.load(std::memory_order_acquire)) {
                SenderLockFreeWorker::free_msg(msg);
                return -1;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    }
}

void SenderLockFreeWorker::run() {
    void* socket = nullptr;
    void* metrics_socket = nullptr;
    std::string metrics_path;
    MetricsData metrics_data;
    auto tid = std::this_thread::get_id();

    std::cout << "Starting Lockfree forward. Sender TID: " << tid << std::endl;
    if (socket_type > 0) {
        socket = create_socket(zmq_ctx, {socket_type, cfg.hwm, cfg.outurl, true});
        zmq_setsockopt(socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
        if (socket_type == ZMQ_REP || socket_type == ZMQ_ROUTER) {
            zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
        }
    }
    if (cfg.metrics) {
        // Note for PUB: with multiple senders this needs edited, TID added or something
        //metrics_socket = create_metrics_socket(metrics_path); // PUB
        metrics_socket = create_metrics_socket(); // PUSH
    }
    while (1) {
        if (socket_type == ZMQ_REP || socket_type == ZMQ_ROUTER) {
            Action ret = receive(socket);
            if (ret == Action::Break) break;
            if (ret == Action::Continue) continue;
        }

        zmq_msg_t* msg = nullptr;
        while (!queue.pop(msg)) {
            if (shutdown_signal.load(std::memory_order_acquire) &&
                queue.read_available() == 0) {
                break;
            }
            std::this_thread::yield();
        }
        if (shutdown_signal.load(std::memory_order_acquire) && msg == nullptr) break;
        int rc = send(socket, msg);
        if (rc < 0) {
            shutdown_signal.store(true, std::memory_order_release);
            break;
        }
        if (cfg.metrics) {
            send_metrics(metrics_data, rc, metrics_socket);
        }
    }
    if (socket_type > 0) {
        zmq_close(socket);
    }
    if (cfg.metrics) {
        cleanup_metrics(tid, metrics_socket, metrics_path);
    }
}

int ReceiverLockFreeWorker::receive(zmq_msg_t* msg, void* socket) {
    int rc = zmq_msg_recv(msg, socket, 0);
    return rc;
}

int EJFatReceiverLockFreeWorker::receive(zmq_msg_t* msg, void* /*socket*/) {
    uint8_t* evtBuf{nullptr};
    size_t evtSize;
    e2sar::EventNum_t evtNum;
    uint16_t dataId;
    // 1000 is a timeout for an event. For now, not configurable (same as in e2sar_perf)
    auto receive_result = reassembler->recvEvent(&evtBuf, &evtSize, &evtNum, &dataId, 1000);

    if (receive_result.has_error() || receive_result.value() == -1)
        return 0;

    auto freef = [](void* data, void* /*hint*/) {
        delete[] static_cast<uint8_t*>(data);
    };
    if (zmq_msg_init_data(msg, evtBuf, evtSize, freef, nullptr) != 0) {
        delete[] evtBuf;
        return -1;
    }
    return static_cast<int>(evtSize);
}

void ReceiverLockFreeWorker::run() {
    void* socket = nullptr;
    auto tid = std::this_thread::get_id();

    void* metrics_socket = nullptr;
    std::string metrics_path;
    MetricsData metrics_data;

    std::cout << "Starting Lockfree forward. Receiver TID: " << tid << std::endl;
    if (socket_type == ZMQ_PULL) {
        socket = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
        zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    }
    if (cfg.metrics) {
        //metrics_socket = create_metrics_socket(metrics_path); // PUB
        metrics_socket = create_metrics_socket(); // PUSH
    }
    bool started_work = false;
    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        int rc = receive(&msg, socket);
        if (socket_type == -1 && rc == 0) {
            // Reassembly error/probably lost event in EJFAT mode, we skip event and go on
            zmq_msg_close(&msg);
            if (shutdown_signal.load(std::memory_order_acquire)) break;
            continue;
        }
        if (rc < 0) {
            zmq_msg_close(&msg);
            int err = zmq_errno();
            if (err == EAGAIN) {
                if (shutdown_signal.load(std::memory_order_acquire)) break;
                if (!started_work) continue;
                if (timeout > 0) {
                    std::cout << "No messages for "
                              << static_cast<double>(timeout)/1000
                              << " seconds. Exiting."
                              << std::endl;
                }
            } else {
                std::cerr << "Error, " << zmq_strerror(err) << " closing threads." << std::endl;
            }
            shutdown_signal.store(true, std::memory_order_release);
            break;
        } else if (cfg.metrics) {
            send_metrics(metrics_data, rc, metrics_socket);
        }
        started_work = true;
        zmq_msg_t* qmsg = new zmq_msg_t();
        zmq_msg_init(qmsg);
        zmq_msg_move(qmsg, &msg);
        while (!queue.push(qmsg)) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            if (shutdown_signal.load(std::memory_order_acquire)) {
                zmq_msg_close(qmsg);
                delete qmsg;
                qmsg = nullptr;
                break;
            }
        }
        if (shutdown_signal.load(std::memory_order_acquire)) {
            break;
        }
    }
    if (socket_type != -1) {
        zmq_close(socket);
    }
    if (cfg.metrics) {
        cleanup_metrics(tid, metrics_socket, metrics_path);
    }
}

static void free_data(void* /*data*/, void* dataowner) {
    delete static_cast<std::shared_ptr<std::vector<uint16_t>>*>(dataowner);
}

void QueueGeneratorLockFreeWorker::run() {
    auto tid = std::this_thread::get_id();
    std::cout << "Starting Data Generator Worker.. TID: " << tid << std::endl;
    size_t datasize = cfg.simdata_size_mb * 512 * 1024;
    size_t datasize_byte = datasize * sizeof(uint16_t);
    auto data = std::make_shared<std::vector<uint16_t>>(datasize, 1);

    while (1) {
        if (shutdown_signal.load(std::memory_order_acquire)) break;
        auto* qmsg = new zmq_msg_t();
        auto* dataowner = new std::shared_ptr<std::vector<uint16_t>>(data);
        zmq_msg_init_data(qmsg, data->data(), datasize_byte, free_data, dataowner);

        while (!queue.push(qmsg)) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            if (shutdown_signal.load(std::memory_order_acquire)) {
                zmq_msg_close(qmsg);
                delete qmsg;
                return;
            }
        }
    }
}

void SinkLockFreeWorker::run() {
    void* socket = nullptr;
    auto tid = std::this_thread::get_id();

    std::cout << "Starting Sink Worker.. TID: " << tid << std::endl;

    socket = create_socket(zmq_ctx, {ZMQ_PULL, cfg.hwm, cfg.inurl, true});
    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

    bool started_work = false;
    uint64_t total_bytes = 0;
    auto last_print_time = std::chrono::steady_clock::now();

    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        int rc = zmq_msg_recv(&msg, socket, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            int err = zmq_errno();
            if (err == EAGAIN) {
                if (shutdown_signal.load(std::memory_order_acquire)) break;
                if (!started_work) continue;
                if (timeout > 0) {
                    std::cout << "No messages for " << static_cast<double>(timeout)/1000 
                              << " seconds. Exiting." << std::endl;
                }
            } else {
                std::cerr << "Error " << zmq_strerror(err) << " closing threads." << std::endl;
            }
            shutdown_signal.store(true, std::memory_order_release);
            break;
        }

        started_work = true;
        total_bytes += rc;
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                now - last_print_time).count();

        if (elapsed >= 1000) {
            double seconds = elapsed / 1000.0;
            double gbs = (static_cast<double>(total_bytes) / (1024 * 1024 * 1024)) / seconds;
            std::cout << gbs << " GB/s" << std::endl;
            total_bytes = 0;
            last_print_time = now;
        }

        zmq_msg_close(&msg);
        if (shutdown_signal.load(std::memory_order_acquire)) {
            break;
        }
    }
  zmq_close(socket);
}

void EJFatReassemblerStatWorker::run() {
    while(1) {
        auto stats = reassembler->getStats();
        std::cout << "Stats:" << std::endl;
        std::cout << "\tTotal Bytes: " << stats.totalBytes << std::endl;
        std::cout << "\tTotal Packets: " << stats.totalPackets << std::endl;
        std::cout << "\tBad RE Header Discards: " << stats.badHeaderDiscards << std::endl;
        std::cout << "\tEvents Received: " << stats.eventSuccess << std::endl;
        std::cout << "\tEvents Lost in reassembly: " << stats.reassemblyLoss << std::endl;
        std::cout << "\tEvents Lost in enqueue: " << stats.enqueueLoss << std::endl;
        std::cout << "\tData Errors: " << stats.dataErrCnt << std::endl;
        if (stats.dataErrCnt > 0)
            std::cout << "\tLast Data Error: " << strerror(stats.lastErrno) << std::endl;
        std::cout << "\tgRPC Errors: " << stats.grpcErrCnt << std::endl << std::endl;
        if (shutdown_signal.load(std::memory_order_acquire)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
