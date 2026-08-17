#include <cassert>
#include <iostream>
#include <cerrno>
#include "../src/threadworker.h"
#include <thread>

class TestThreadWorker : public ThreadWorker {
public:
    using ThreadWorker::ThreadWorker;
    void run() override {}
    using ThreadWorker::create_socket;
    using ThreadWorker::send_receive;
};

void Test_create_socket_bind_pull() {
    void* zmq_ctx = zmq_ctx_new();
    assert(zmq_ctx != nullptr);
    Config cfg;
    cfg.hwm = 1;

    TestThreadWorker worker(zmq_ctx, cfg);
    SocketConfig config{ZMQ_PULL, cfg.hwm, "inproc://test1", true};

    void* socket = worker.create_socket(zmq_ctx, config);
    assert(socket != nullptr);

    int actual_hwm = 0;
    size_t actual_hwm_size = sizeof(actual_hwm);
    int rc = zmq_getsockopt(socket, ZMQ_RCVHWM, &actual_hwm, &actual_hwm_size);
    assert(rc == 0);
    assert(actual_hwm == cfg.hwm);

    void* secondary_socket = zmq_socket(zmq_ctx, ZMQ_PULL);
    assert(secondary_socket != nullptr);
    rc = zmq_bind(secondary_socket, config.url.c_str());
    assert(rc == -1);
    assert(zmq_errno() == EADDRINUSE);

    zmq_close(socket);
    zmq_close(secondary_socket);
    zmq_ctx_term(zmq_ctx);
    zmq_ctx = nullptr;
}

void Test_create_socket_connect_push() {
    void* zmq_ctx = zmq_ctx_new();
    assert(zmq_ctx != nullptr);
    Config cfg;
    cfg.hwm = 2;

    TestThreadWorker worker(zmq_ctx, cfg);
    SocketConfig config{ZMQ_PUSH, cfg.hwm, "inproc://test2", false};

    void* secondary_socket = zmq_socket(zmq_ctx, ZMQ_PULL);
    assert(zmq_bind(secondary_socket, "inproc://test2") == 0);

    void* socket = worker.create_socket(zmq_ctx, config);
    assert(socket != nullptr);

    int actual_hwm = 0;
    size_t actual_hwm_size = sizeof(actual_hwm);
    int rc = zmq_getsockopt(socket, ZMQ_SNDHWM, &actual_hwm, &actual_hwm_size);
    assert(rc == 0);
    assert(actual_hwm == cfg.hwm);

    zmq_close(socket);
    zmq_close(secondary_socket);
    zmq_ctx_term(zmq_ctx);
    zmq_ctx = nullptr;
}

void Test_send_receive() {
    void* zmq_ctx = zmq_ctx_new();
    assert(zmq_ctx != nullptr);
    Config cfg;

    TestThreadWorker worker(zmq_ctx, cfg);

    const char* url_in = "inproc://sr_test_in";
    const char* url_out = "inproc://sr_test_out";

    void* sender1 = zmq_socket(zmq_ctx, ZMQ_PUSH);
    assert(zmq_bind(sender1, url_in) == 0);

    void* receiver1 = zmq_socket(zmq_ctx, ZMQ_PULL);
    assert(zmq_connect(receiver1, url_in) == 0);

    void* sender2 = zmq_socket(zmq_ctx, ZMQ_PUSH);
    assert(zmq_bind(sender2, url_out) == 0);

    void* receiver2 = zmq_socket(zmq_ctx, ZMQ_PULL);
    assert(zmq_connect(receiver2, url_out) == 0);

    const char* expected_data = "Test123";
    int send_rc = zmq_send(sender1, expected_data, strlen(expected_data), 0);
    assert(send_rc > 0);

    int rc = worker.send_receive(receiver1, sender2);
    assert(rc == 0);

    char buffer[32] = {0};
    int recv_rc = zmq_recv(receiver2, buffer, sizeof(buffer) - 1, 0);
    assert(recv_rc > 0);
    assert(std::string(buffer) == expected_data);

    zmq_close(sender1);
    zmq_close(receiver1);
    zmq_close(sender2);
    zmq_close(receiver2);
    zmq_ctx_term(zmq_ctx);
}

int main() {
    std::cout << "" << std::endl;

    std::cout << "Testing socket creation - Bind Pull ..";
    Test_create_socket_bind_pull();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing socket creation - Connect Push ..";
    Test_create_socket_connect_push();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "Testing send_receive ..";
    Test_send_receive();
    std::cout << "\033[32m [PASS]\033[0m\n";

    std::cout << "" << std::endl;
    return 0;
}