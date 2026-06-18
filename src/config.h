#ifndef CONFIG_H
#define CONFIG_H
#pragma once

//#include <fstream>
//#include <iostream>
#include <string>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct Config
{
    std::string inurl;
    std::string outurl;
    std::string workerurl = "inproc://worker";
    int helper_threads;
    int io_threads;
    int hwm;
    int type;
    bool verbose;
};

void load_config(const std::string& fname, Config& conf);

#endif
