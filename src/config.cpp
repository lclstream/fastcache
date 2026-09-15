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

    auto fastcache_opts = jobj.value("fastcache", nlohmann::json::object());
    auto metrics_opts = jobj.value("metrics", nlohmann::json::object());
    auto ejfat_opts = jobj.value("ejfat", nlohmann::json::object());

    cfg.inurl = fastcache_opts.value("inurl", "tcp://localhost:19432");
    cfg.outurl = fastcache_opts.value("outurl", "tcp://localhost:19439");
    cfg.workerurl = fastcache_opts.value("workerurl", "inproc://worker");
    cfg.type = fastcache_opts.value("type", 4);
    cfg.helper_threads = fastcache_opts.value("helper_threads", 0);
    cfg.zmq_io_threads = fastcache_opts.value("zmq_io_threads", 16);
    cfg.hwm = fastcache_opts.value("hwm", 10);
    cfg.timeout = fastcache_opts.value("timeout", -1);
    cfg.verbose = fastcache_opts.value("verbose", false);
    cfg.dataMB = fastcache_opts.value("dataMB", 32);

    cfg.metrics = metrics_opts.value("metrics", false);
    cfg.metrics_cache_id = metrics_opts.value("cache_id", 1);
    cfg.metrics_interval = metrics_opts.value("metrics_interval", 10000);

    cfg.ejfat_useLB = ejfat_opts.value("ejfat_useLB", true);
    cfg.ejfat_mtu = ejfat_opts.value("mtu", 9000);
    // setsockopt doubles the buffer size (this is assuming wmem_max is 2GB)
    int ejfat_rawBufSize = ejfat_opts.value("sndbufsize", 16777216);
    cfg.ejfat_sndbufsize = (ejfat_rawBufSize > 1073741823LL) ?
                                        (ejfat_rawBufSize / 2) : (ejfat_rawBufSize);
    cfg.ejfat_rateGbps = ejfat_opts.value("rateGbps", -1.0);
    cfg.ejfat_numSendSockets = ejfat_opts.value("numSendSockets", 8);
    cfg.dataSimulatorThreads = ejfat_opts.value("dataSimulatorThreads", 1);
    cfg.ejfat_queueSize = ejfat_opts.value("queueSize", 40);
    cfg.ejfat_dataId = ejfat_opts.value("dataId", 1);

    if (cfg.type == 7 || cfg.type == 8) {
        const char* uri_result = std::getenv("EJFAT_URI");
        if (!uri_result) {
            std::cerr << "Error: EJFAT_URI environment variable needs to be set." << std::endl;
            std::exit(1);
        }
        cfg.ejfat_uri = uri_result;
    }

    if (cfg.metrics_interval <= 0) {
       std::cerr << "Fatal: metrics_interval must be bigger than 0.\n";
        std::exit(1);
    }
}
