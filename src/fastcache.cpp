#include <cstdlib>
#include <string>
#include <zmq.h>
#include <unistd.h>
#include "cacheserver.h"
#include "config.h"
#include <csignal>

std::atomic<bool> shutdown_signal(false);

void signal_handler(int signum) {
    if (signum == SIGINT) {
        shutdown_signal.store(true, std::memory_order_relaxed);
    }
}

int main(int argc, char* argv[]) {

    std::signal(SIGINT, signal_handler);
    Config cfg;
    std::string fname;

    if (argc < 2) {
        fname = "config/default.json";
    } else {
        fname = argv[1];
    }

    load_config(fname, cfg);
    CacheServer serv(cfg, shutdown_signal);
    serv.run();

    return 0;
}
