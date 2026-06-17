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
    cfg.helper_threads = jobj.value("helper_threads", 0);
    cfg.num_workers = jobj.value("num_workers", 0);
    cfg.type = jobj.value("type", 1);
    cfg.io_threads = jobj.value("io_threads", 1);
    cfg.hwm = jobj.value("hwm", 1);
    cfg.verbose = jobj.value("verbose", false);

    /* TODO error handling here instead of this
    bool has_out = jobj.contains("outurl");
    bool has_req = jobj.contains("requrl");

    if (has_out == has_req) {
        throw std::runtime_error(
            "Config error: exactly only one of 'outurl' or 'requrl' must be present in config."
        );
    }
        */

}
