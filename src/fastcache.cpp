#include <cstdlib>
#include <string>
#include <zmq.h>
#include <unistd.h>
#include "cacheserver.h"
#include "config.h"
#include <csignal>

std::atomic<bool> shutdown(false);

void signal_handler(int signum) {
    if (signum == SIGINT) {
        shutdown.store(true, std::memory_order_relaxed);
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
    CacheServer serv(cfg, shutdown);
    usleep(500);
    serv.Run();

    return 0;
}
