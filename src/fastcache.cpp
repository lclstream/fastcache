#include <cstdlib>
#include <string>
#include <zmq.h>
#include <unistd.h>
#include "cacheserver.h"
#include "config.h"


int main(int argc, char* argv[]) {

    Config cfg;
    std::string fname;

    if (argc < 2) {
        fname = "config/default.json";
    } else {
        fname = argv[1];
    }

    load_config(fname, cfg);

    CacheServer serv(cfg);

    usleep(1000);

    serv.Run();

}
