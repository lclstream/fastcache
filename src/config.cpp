#include <iostream>
#include <fstream>
#include "config.h"

void load_config(const std::string& fname, Config& cfg) {
    std::ifstream file(fname);
    if (!file) {
        std::cout << "Couldnt open file " << fname << std::endl;
    }

    json jobj;
    file >> jobj;

    cfg.inurl = jobj.at("inurl").get<std::string>();
    cfg.outurl = jobj.at("outurl").get<std::string>();
    cfg.num_workers = jobj.value("num_workers", 1);
    cfg.io_threads = jobj.value("io_threads", 1);
    cfg.hwm = jobj.value("hwm", 1);
}
