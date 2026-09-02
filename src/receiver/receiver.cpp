// usage: ./build/receiver <address> <port>
// receives EJFAT events and prints basic information about them

#include <e2sar.hpp>
#include <signal.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <cstdlib>

void exit_error() {
    std::cerr << "receiver receives EJFAT events and prints basic info\n";
    std::cerr << "it is configured via the EJFAT_URI envvar\n";
    std::cerr << "usage: receiver <address> <port>\n";
    exit(1);
}

// Build an e2sar::Reassembler or exit by parsing argv[1] as address and argv[2] as port
void build_reassembler_from_env_and_args(e2sar::Reassembler ** reassembler, const char * address, const char * port) {
    boost::system::error_code ec;
    boost::asio::ip::address listen_ip = boost::asio::ip::make_address(address, ec);
    if (ec) {
        std::cerr << "could not parse address: " << ec.message() << address;
        exit_error();
    }
    int listen_port = 0;
    try {
        std::size_t pos;
        listen_port = std::stoi(port, &pos);
        if (listen_port < 0 || listen_port > 65535) {
            std::cerr << "port number out of range (0-65535): " << port << std::endl;
            exit_error();
        }
    } catch (std::invalid_argument const &ex) {
        std::cerr << "invalid port number: " << port << std::endl;
        exit_error();
    } catch (std::out_of_range const &ex) {
        std::cerr << "port number out of range (0-65535): " << port <<  std::endl;
        exit_error();
    }
    // parse EJFAT_URI environment variable
    auto uri_result = e2sar::EjfatURI::getFromEnv("EJFAT_URI", e2sar::EjfatURI::TokenType::instance, false);
    if (!uri_result) {
        std::cerr << "error with EJFAT_URI environment variable: " << uri_result.error().message() << "\n";
        exit_error();
    }
    auto uri = uri_result.value();

    e2sar::Reassembler::ReassemblerFlags reassembler_flags;
    reassembler_flags.useCP = true;  // set to false for local test
    reassembler_flags.withLBHeader = !reassembler_flags.useCP;
    reassembler_flags.reportStats = true;
    reassembler_flags.rcvSocketBufSize = 16*1024*1024;
    try {
        *reassembler = new e2sar::Reassembler(uri, listen_ip, listen_port, /*threads=*/2, reassembler_flags);
    } catch (const e2sar::E2SARException& ex) {
        std::cerr << "error when creating reassembler: " << ex.what() << "\n";
        delete reassembler;
        exit_error();
    }
}

bool is_running = true;

// Starts the reassembler threads, then registers with the control plane
void start_and_register_reassembler(e2sar::Reassembler* reassembler) {
    boost::system::error_code ec;
    try {
        auto open_result = reassembler->openAndStart();
        if (open_result.has_error()) {
            std::cerr << "error starting reassembler threads: " << open_result.error().message() << "\n";
            exit_error();
        }
    } catch (const e2sar::E2SARException& ex) {
        std::cerr << "error starting reassembler threads: " << ex.what() << "\n";
        delete reassembler;
        exit_error();
    }
    
    std::string hostname = boost::asio::ip::host_name(ec);
    if (ec) {
        std::cerr << "error getting hostname: " << ec.message();
        exit_error();
    }
    auto register_status = reassembler->registerWorker(hostname);
    if (register_status.has_error()) {
        std::cerr << "error registering worker: " << register_status.error().message() << "\n";
        exit_error();
    }
}

void deregister_and_stop_reassembler(e2sar::Reassembler* reassembler) {
    auto deregister_result = reassembler->deregisterWorker();
    if (deregister_result.has_error()) {
        std::cerr << "error deregistering worker on exit: " << deregister_result.error().message() << std::endl;
    }
    reassembler->stopThreads();
}

void sigint_handler(int sig) {
    std::cout << "deregistering worker" << std::endl;
    is_running = false;
}

int main(int argc, const char* argv[]) {
    //
    // parse EJFAT_URI and command line args to build the e2sar::Reassembler
    //
    if (argc < 3) {
        exit_error();
    }
    const char* listen_address = argv[1];
    const char* listen_port = argv[2];
    e2sar::Reassembler* reassembler{nullptr};
    build_reassembler_from_env_and_args(&reassembler, listen_address, listen_port);
    start_and_register_reassembler(reassembler);
    //
    // receive events until SIGINT, providing basic information about them
    //
    u_int8_t *event_data{nullptr};
    size_t event_len = 0;
    e2sar::EventNum_t event_num;
    u_int16_t event_data_id = 0x0001;

    signal(SIGINT, sigint_handler); // will set is_running false when SIGINT
    std::cout << "Starting..." << std::endl;
    while (is_running) {
        auto reassemblerStatus = reassembler->recvEvent(&event_data, &event_len, &event_num, &event_data_id, 10000);
        if ((!reassemblerStatus.has_error()) && event_len > 0) {
            if (event_num % 1000 == 0) {
                auto stats = reassembler->getStats();
                uint16_t lost = stats.reassemblyLoss;
                float ratio = float(lost)/float(event_num);
                std::cout << "Loss: " << lost << "Total: " << event_num << " Ratio: " << ratio*100 << "%"  << std::endl;
            }
        }
        /*     std::cout << listen_address << ":" << listen_port << ":  start event num " << event_num << " with data id " << event_data_id << " and length " << event_len << " bytes" << std::endl;
             // This is where you would do data processing, instead of sleeping
             boost::this_thread::sleep_for(boost::chrono::milliseconds(170));
             std::cout << listen_address << ":" << listen_port << ": finish event num " << event_num << " with data id " << event_data_id << " and length " << event_len << " bytes" << std::endl;
        } else {
            std::cerr << "Some error: " << reassemblerStatus.error().message() << std::endl;
        }*/
    }

    //
    // cleanly exit
    //
    deregister_and_stop_reassembler(reassembler);
    return 0;
}
