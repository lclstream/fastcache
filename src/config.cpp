#include <iostream>
#include <fstream>
#include "config.h"

void load_config(const std::string& fname, Config& cfg) {
    std::ifstream file(fname);
    std::cout << "\nUsing config file: " << fname << std::endl;
    if (!file) {
        std::cout << "Couldnt open config file " << fname << std::endl;
    }

    json jobj;
    file >> jobj;

    cfg.inurl = jobj.at("inurl").get<std::string>();
    cfg.outurl = jobj.value("outurl", "");
    cfg.type = jobj.value("type", 4);
    cfg.helper_threads = jobj.value("helper_threads", 0);
    cfg.io_threads = jobj.value("io_threads", 16);
    cfg.hwm = jobj.value("hwm", 10);
    cfg.timeout = jobj.value("timeout", -1);
    cfg.verbose = jobj.value("verbose", false);
    cfg.metrics = jobj.value("metrics", false);

    // TODO error handling here 

}
