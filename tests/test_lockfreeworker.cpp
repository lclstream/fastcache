#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstring>
#include <cerrno>
#include <boost/lockfree/spsc_queue.hpp>
#include <zmq.h>

#include "../src/threadworker.h"

class TestReplySenderWorker : public ReplySenderLockFreeWorker {
public:
    using ReplySenderLockFreeWorker::ReplySenderLockFreeWorker;
    using ReplySenderLockFreeWorker::receive;
};

class TestRouterSenderWorker : public RouterSenderLockFreeWorker {
public:
    using RouterSenderLockFreeWorker::RouterSenderLockFreeWorker;
    using RouterSenderLockFreeWorker::receive;
    using RouterSenderLockFreeWorker::send;
};

class TestSenderWorker : public SenderLockFreeWorker {
public:
    using SenderLockFreeWorker::SenderLockFreeWorker;
    using SenderLockFreeWorker::receive;
    using SenderLockFreeWorker::send;
};

void Test_SenderLockFreeWorker_receive() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    TestSenderWorker worker(zmq_ctx, cfg, queue, shutdown, ZMQ_PUSH);
    Action act = worker.receive(nullptr);
    assert(act == Action::Resume);

    zmq_ctx_term(zmq_ctx);
}

void Test_ReplySenderLockFreeWorker_receive1() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    TestReplySenderWorker worker(zmq_ctx, cfg, queue, shutdown);

    void* req_socket = zmq_socket(zmq_ctx, ZMQ_REQ);
    void* rep_socket = zmq_socket(zmq_ctx, ZMQ_REP);
    assert(zmq_bind(rep_socket, "inproc://reply_test") == 0);
    assert(zmq_connect(req_socket, "inproc://reply_test") == 0);

    const char* req_str = "Test123";
    zmq_send(req_socket, req_str, strlen(req_str), 0);

    Action act = worker.receive(rep_socket);
    assert(act == Action::Resume);

    zmq_close(req_socket);
    zmq_close(rep_socket);
    zmq_ctx_term(zmq_ctx);
}

void Test_ReplySenderLockFreeWorker_receive2() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    TestReplySenderWorker worker(zmq_ctx, cfg, queue, shutdown);

    void* rep_socket = zmq_socket(zmq_ctx, ZMQ_REP);
    int timeout = 10;
    zmq_setsockopt(rep_socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    assert(zmq_bind(rep_socket, "inproc://reply_test") == 0);

    // 1. without shutdown -> Action::Continue
    Action act = worker.receive(rep_socket);
    assert(act == Action::Continue);

    // 2. with shutdown -> Action::Break
    shutdown.store(true, std::memory_order_release);
    act = worker.receive(rep_socket);
    assert(act == Action::Break);

    zmq_close(rep_socket);
    zmq_ctx_term(zmq_ctx);
}

void Test_RouterSenderLockFreeWorker_receive() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    TestRouterSenderWorker worker(zmq_ctx, cfg, queue, shutdown);

    void* dealer_socket = zmq_socket(zmq_ctx, ZMQ_DEALER);
    void* router_socket = zmq_socket(zmq_ctx, ZMQ_ROUTER);
    assert(zmq_bind(router_socket, "inproc://router_test") == 0);
    assert(zmq_connect(dealer_socket, "inproc://router_test") == 0);

    zmq_send(dealer_socket, "", 0, ZMQ_SNDMORE);
    zmq_send(dealer_socket, "Test123", 7, 0);

    Action act = worker.receive(router_socket);
    assert(act == Action::Resume);

    zmq_close(dealer_socket);
    zmq_close(router_socket);
    zmq_ctx_term(zmq_ctx);
}

void Test_SenderLockFreeWorker_send() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    TestSenderWorker worker(zmq_ctx, cfg, queue, shutdown, ZMQ_PUSH);

    void* push_socket = zmq_socket(zmq_ctx, ZMQ_PUSH);
    void* pull_socket = zmq_socket(zmq_ctx, ZMQ_PULL);
    assert(zmq_bind(push_socket, "inproc://sender_test") == 0);
    assert(zmq_connect(pull_socket, "inproc://sender_test") == 0);

    const char* data = "Test123";
    int size = 7;

    zmq_msg_t msg;
    zmq_msg_init_size(&msg, size);
    memcpy(zmq_msg_data(&msg), data, size);

    int rc = worker.send(push_socket, &msg);
    assert(rc == size);

    char buf[16] = {0};
    int recv_bytes = zmq_recv(pull_socket, buf, sizeof(buf), 0);
    assert(recv_bytes == size);
    assert(memcmp(buf, data, size) == 0);

    zmq_msg_close(&msg);
    zmq_close(push_socket);
    zmq_close(pull_socket);
    zmq_ctx_term(zmq_ctx);
}

void Test_RouterSenderLockFreeWorker_send() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    TestRouterSenderWorker worker(zmq_ctx, cfg, queue, shutdown);

    void* router_socket = zmq_socket(zmq_ctx, ZMQ_ROUTER);
    void* dealer_socket = zmq_socket(zmq_ctx, ZMQ_DEALER);
    assert(zmq_bind(router_socket, "inproc://router_test") == 0);
    assert(zmq_connect(dealer_socket, "inproc://router_test") == 0);

    const char* data = "Test123";
    int size = 7;

    zmq_send(dealer_socket, "", 0, ZMQ_SNDMORE);
    zmq_send(dealer_socket, data, size, 0);

    Action act = worker.receive(router_socket);
    assert(act == Action::Resume);

    zmq_msg_t reply_msg;
    zmq_msg_init_size(&reply_msg, 7);
    memcpy(zmq_msg_data(&reply_msg), data, size);

    int send_rc = worker.send(router_socket, &reply_msg);
    assert(send_rc == size);

    char empty_buf[16] = {0};
    char data_buf[16] = {0};
    zmq_recv(dealer_socket, empty_buf, sizeof(empty_buf), 0);
    int more = 0;
    size_t more_len = sizeof(more);
    zmq_getsockopt(dealer_socket, ZMQ_RCVMORE, &more, &more_len);
    assert(more == 1);

    zmq_recv(dealer_socket, data_buf, sizeof(data_buf), 0);
    assert(memcmp(data_buf, data, size) == 0);

    zmq_msg_close(&reply_msg);
    zmq_close(router_socket);
    zmq_close(dealer_socket);
    zmq_ctx_term(zmq_ctx);
}

void Test_ReceiverLockFreeWorker_run_timeout() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    cfg.hwm = 10;
    cfg.inurl = "inproc://timeout_test";
    cfg.metrics = false;
    cfg.timeout = 50;

    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    void* socket = zmq_socket(zmq_ctx, ZMQ_PUSH);
    assert(zmq_connect(socket, cfg.inurl.c_str()) == 0);

    ReceiverLockFreeWorker receiver(zmq_ctx, cfg, queue, shutdown);

    std::thread thread1([&]() { receiver.run(); });

    zmq_send(socket, "Test123", 7, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    zmq_msg_t* qmsg = nullptr;
    while (!queue.pop(qmsg)) { std::this_thread::yield(); }
    zmq_msg_close(qmsg);
    delete qmsg;

    thread1.join();

    assert(shutdown.load() == true); // test shutdown flag too

    zmq_close(socket);
    zmq_ctx_term(zmq_ctx);
}

void Test_SenderLockFreeWorker_run_queuedrain() {
    void* zmq_ctx = zmq_ctx_new();
    Config cfg;
    cfg.hwm = 10;
    cfg.outurl = "inproc://drain_test";
    cfg.metrics = false;

    boost::lockfree::spsc_queue<zmq_msg_t*, boost::lockfree::capacity<100>> queue;
    std::atomic<bool> shutdown{false};

    void* socket = zmq_socket(zmq_ctx, ZMQ_PULL);
    assert(zmq_connect(socket, cfg.outurl.c_str()) == 0);

    for (int i = 0; i < 100; ++i) {
        zmq_msg_t* msg = new zmq_msg_t();
        zmq_msg_init_size(msg, 7);
        memcpy(zmq_msg_data(msg), "Test123", 7);
        queue.push(msg);
    }

    shutdown.store(true, std::memory_order_release);

    SenderLockFreeWorker sender(zmq_ctx, cfg, queue, shutdown, ZMQ_PUSH);
    std::thread thread1([&]() { sender.run(); });

    char buf[16] = {0};
    for (int i = 0; i < 100; ++i) {
    int recv_bytes = zmq_recv(socket, buf, sizeof(buf), 0);
    assert(recv_bytes == 7);
}

    thread1.join();

    zmq_close(socket);
    zmq_ctx_term(zmq_ctx);
}

int main() {
    std::cout << "" << std::endl;

    std::cout << "Testing SenderLockFreeWorker::receive ..";
    Test_SenderLockFreeWorker_receive();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing ReplySenderLockFreeWorker::receive ..";
    Test_ReplySenderLockFreeWorker_receive1();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing ReplySenderLockFreeWorker::receive fail modes (EAGAIN) ..";
    Test_ReplySenderLockFreeWorker_receive2();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing RouterSenderLockFreeWorker::receive ..";
    Test_RouterSenderLockFreeWorker_receive();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing SenderLockFreeWorker::send ..";
    Test_SenderLockFreeWorker_send();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing RouterSenderLockFreeWorker::send ..";
    Test_RouterSenderLockFreeWorker_send();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing ReceiverLockFreeWorker::run (timeout test) ..";
    Test_ReceiverLockFreeWorker_run_timeout();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing SenderLockFreeWorker::run (Shutdown queue drain) ..";
    Test_SenderLockFreeWorker_run_queuedrain();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "" << std::endl;
    return 0;
}