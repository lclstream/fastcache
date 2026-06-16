// Single-process diagnostic for the "send-only is fine, send+recv collapses"
// problem on the S3DF DTN.
//
// It answers ONE question: when bidirectional ZMQ traffic slows down, is it
//   (a) the DRAM bandwidth wall (physics -> only fewer copies help), or
//   (b) core / cache / io_thread contention (software-fixable: pinning,
//       more io_threads, dropping the relay hop).
//
// How it works
// ------------
//   1. memcpy calibration: T parallel threads memcpy between large (> L3)
//      buffers to measure this machine's usable DRAM bandwidth ceiling.
//   2. ZMQ loopback pipelines: K independent PUSH->PULL pipelines over
//      tcp://127.0.0.1 . TCP loopback still performs the two kernel copies
//      through DRAM (unlike inproc, which is zero-copy), so it reproduces the
//      real memory traffic of the relay WITHOUT needing a second host or NIC.
//   3. Optional relay mode: PUSH -> (PULL->PUSH relay) -> PULL, mirroring the
//      fastcache architecture, to measure the cost of the extra hop.
//
// Interpreting results
//   A relay moves each frame through ~2 DRAM passes. So:
//     ZMQ aggregate ~= (memcpy ceiling) / 2   => BANDWIDTH bound (physics).
//     ZMQ aggregate <<  (memcpy ceiling) / 2  => CONTENTION/overhead bound.
//   Compare same-L3 vs split-L3 vs spread pinning: if pinning changes the
//   number a lot, you were contention bound; if not, you are at the wall.
//
// Linux-only features (thread pinning, L3 topology) degrade to no-ops
// elsewhere so it still builds/runs on the macOS dev box.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zmq.h>

#if defined(__linux__)
#include <arpa/inet.h>
#include <fcntl.h>
#include <fstream>
#include <linux/errqueue.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

double seconds_since(Clock::time_point t0) {
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

// ---------------------------------------------------------------------------
// CPU affinity (Linux only; no-op elsewhere)
// ---------------------------------------------------------------------------
void pin_this_thread(const std::vector<int> &cores) {
#if defined(__linux__)
    if (cores.empty())
        return;
    cpu_set_t set;
    CPU_ZERO(&set);
    for (int c : cores)
        CPU_SET(c, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)cores;
#endif
}

// Cores this process is actually permitted to run on. On a shared DTN the job
// is usually confined to a cgroup/cpuset, so this can be a small subset of the
// machine. Returns empty on non-Linux (meaning "unknown / all allowed").
//
// This matters because libzmq's ZMQ_THREAD_AFFINITY_CPU_ADD does an
// errno_assert() on pthread_setaffinity_np and ABORTS if asked to pin an
// io-thread to a core outside this set. We must never feed it a forbidden core.
std::vector<int> allowed_cpus() {
    std::vector<int> out;
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int c = 0; c < CPU_SETSIZE; ++c)
            if (CPU_ISSET(c, &set))
                out.push_back(c);
    }
#endif
    return out;
}

// Drop any cores not in `allowed` from each group, and drop groups that become
// empty. If `allowed` is empty (unknown), groups pass through unchanged.
std::vector<std::vector<int>>
filter_groups_to_allowed(const std::vector<std::vector<int>> &groups,
                         const std::vector<int> &allowed) {
    if (allowed.empty())
        return groups;
    std::vector<std::vector<int>> out;
    for (const auto &g : groups) {
        std::vector<int> keep;
        for (int c : g)
            if (std::find(allowed.begin(), allowed.end(), c) != allowed.end())
                keep.push_back(c);
        if (!keep.empty())
            out.push_back(std::move(keep));
    }
    return out;
}

// Detect L3 groups (each list of cores sharing an L3) from sysfs.
// Returns groups of *physical* cores only (lowest SMT sibling per core id set
// is kept as-is; we simply return the shared_cpu_list verbatim, callers pick).
std::vector<std::vector<int>> detect_l3_groups() {
    std::vector<std::vector<int>> groups;
#if defined(__linux__)
    // Parse /sys/devices/system/cpu/cpu*/cache/index3/shared_cpu_list and
    // deduplicate identical lists.
    std::vector<std::string> seen;
    for (int cpu = 0; cpu < 1024; ++cpu) {
        std::ostringstream path;
        path << "/sys/devices/system/cpu/cpu" << cpu
             << "/cache/index3/shared_cpu_list";
        std::ifstream f(path.str());
        if (!f.good())
            continue;
        std::string list;
        std::getline(f, list);
        if (list.empty())
            continue;
        bool dup = false;
        for (auto &s : seen)
            if (s == list) {
                dup = true;
                break;
            }
        if (dup)
            continue;
        seen.push_back(list);

        // Parse "0-3,32-35" into ints.
        std::vector<int> cores;
        std::stringstream ss(list);
        std::string tok;
        while (std::getline(ss, tok, ',')) {
            auto dash = tok.find('-');
            if (dash == std::string::npos) {
                cores.push_back(std::stoi(tok));
            } else {
                int lo = std::stoi(tok.substr(0, dash));
                int hi = std::stoi(tok.substr(dash + 1));
                for (int i = lo; i <= hi; ++i)
                    cores.push_back(i);
            }
        }
        groups.push_back(std::move(cores));
    }
#endif
    return groups;
}

// Keep only physical cores (< total_physical) from a group, dropping SMT
// siblings so copy-heavy threads do not share a physical core.
std::vector<int> physical_only(const std::vector<int> &group, int n_physical) {
    std::vector<int> out;
    for (int c : group)
        if (c < n_physical)
            out.push_back(c);
    return out;
}

// ---------------------------------------------------------------------------
// 1. memcpy bandwidth calibration
// ---------------------------------------------------------------------------
double memcpy_bandwidth_gbps(int n_threads, size_t buf_bytes, double secs,
                             const std::vector<std::vector<int>> &l3,
                             int n_physical) {
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_bytes{0};
    // Threads decrement this after pre-touching their buffers; the timed loop
    // starts only once all have arrived, so allocation is excluded.
    std::atomic<int> start_barrier{n_threads};
    std::vector<std::thread> ts;

    for (int t = 0; t < n_threads; ++t) {
        ts.emplace_back([&, t]() {
            // Spread threads across L3 groups, physical cores only.
            if (!l3.empty()) {
                auto grp = physical_only(l3[t % l3.size()], n_physical);
                if (!grp.empty())
                    pin_this_thread({grp[(t / (int)l3.size()) % grp.size()]});
            }
            std::vector<char> src(buf_bytes), dst(buf_bytes);
            // Pre-touch (allocate + fault in) BEFORE the timed region so
            // allocation cost is not charged against the measurement window.
            std::memset(src.data(), 1, buf_bytes);
            std::memset(dst.data(), 2, buf_bytes);
            start_barrier.fetch_sub(1, std::memory_order_acq_rel);
            while (start_barrier.load(std::memory_order_acquire) > 0) {
                std::this_thread::yield();
            }
            uint64_t local = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                std::memcpy(dst.data(), src.data(), buf_bytes);
                // Touch dst so the compiler cannot elide the copy.
                dst[local % buf_bytes] ^= 1;
                local += buf_bytes;
            }
            total_bytes.fetch_add(local, std::memory_order_relaxed);
        });
    }

    // Wait until all worker threads have finished pre-touching their buffers
    // before starting the clock.
    while (start_barrier.load(std::memory_order_acquire) > 0)
        std::this_thread::yield();
    auto t0 = Clock::now();
    std::this_thread::sleep_for(std::chrono::duration<double>(secs));
    stop.store(true);
    for (auto &th : ts)
        th.join();
    double elapsed = seconds_since(t0);
    // memcpy reads buf + writes buf => 2x DRAM traffic per copied byte.
    double moved = (double)total_bytes.load();
    return (moved / elapsed) / 1e9; // GB/s of *copied* bytes (dst writes)
}

// ---------------------------------------------------------------------------
// 2. ZMQ loopback pipeline
// ---------------------------------------------------------------------------
struct PipelineCfg {
    size_t frame_bytes;
    int io_threads;
    int hwm;
    double secs;
    bool relay;                 // insert a PULL->PUSH relay hop
    std::vector<int> prod_cores;
    std::vector<int> cons_cores;
    std::vector<int> relay_cores;
    std::vector<int> ioctx_cores; // pin context io_threads here
    int base_port;
};

// One pipeline = its own context(s) + producer + consumer (+ optional relay).
// Returns bytes received by the consumer.
uint64_t run_pipeline(const PipelineCfg &c, std::atomic<bool> &stop) {
    void *ctx = zmq_ctx_new();
#if defined(__linux__)
    for (int core : c.ioctx_cores)
        zmq_ctx_set(ctx, ZMQ_THREAD_AFFINITY_CPU_ADD, core);
#endif
    zmq_ctx_set(ctx, ZMQ_IO_THREADS, c.io_threads);

    const std::string url_a =
        "tcp://127.0.0.1:" + std::to_string(c.base_port);
    const std::string url_b =
        "tcp://127.0.0.1:" + std::to_string(c.base_port + 1);

    std::atomic<uint64_t> received{0};

    const int timeo_ms = 100; // wake periodically to observe `stop`
    const int linger = 0;     // do not block on close/term

    // Consumer (PULL) binds; producer connects. With a relay, the relay sits
    // between: producer -> url_a -> relay -> url_b -> consumer.
    void *consumer = zmq_socket(ctx, ZMQ_PULL);
    int hwm = c.hwm;
    zmq_setsockopt(consumer, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(consumer, ZMQ_RCVTIMEO, &timeo_ms, sizeof(timeo_ms));
    zmq_setsockopt(consumer, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_bind(consumer, c.relay ? url_b.c_str() : url_a.c_str());

    std::thread cons([&]() {
        pin_this_thread(c.cons_cores);
        zmq_msg_t msg;
        while (!stop.load(std::memory_order_relaxed)) {
            zmq_msg_init(&msg);
            int rc = zmq_msg_recv(&msg, consumer, 0);
            if (rc >= 0)
                received.fetch_add((uint64_t)zmq_msg_size(&msg),
                                   std::memory_order_relaxed);
            zmq_msg_close(&msg);
            // rc < 0 with EAGAIN just means the recv timed out; loop and
            // re-check stop.
        }
    });

    void *relay_in = nullptr;
    void *relay_out = nullptr;
    std::thread relay_thread;
    if (c.relay) {
        relay_in = zmq_socket(ctx, ZMQ_PULL);
        relay_out = zmq_socket(ctx, ZMQ_PUSH);
        zmq_setsockopt(relay_in, ZMQ_RCVHWM, &hwm, sizeof(hwm));
        zmq_setsockopt(relay_out, ZMQ_SNDHWM, &hwm, sizeof(hwm));
        zmq_setsockopt(relay_in, ZMQ_RCVTIMEO, &timeo_ms, sizeof(timeo_ms));
        zmq_setsockopt(relay_out, ZMQ_SNDTIMEO, &timeo_ms, sizeof(timeo_ms));
        zmq_setsockopt(relay_in, ZMQ_LINGER, &linger, sizeof(linger));
        zmq_setsockopt(relay_out, ZMQ_LINGER, &linger, sizeof(linger));
        zmq_bind(relay_in, url_a.c_str());
        zmq_connect(relay_out, url_b.c_str());
        relay_thread = std::thread([&]() {
            pin_this_thread(c.relay_cores);
            zmq_msg_t msg;
            while (!stop.load(std::memory_order_relaxed)) {
                zmq_msg_init(&msg);
                int rc = zmq_msg_recv(&msg, relay_in, 0);
                if (rc >= 0) {
                    // Keep retrying the send (timeout-bounded) until it goes
                    // or we are asked to stop, so we never drop on the floor.
                    while (zmq_msg_send(&msg, relay_out, 0) < 0 &&
                           !stop.load(std::memory_order_relaxed)) {
                    }
                }
                zmq_msg_close(&msg);
            }
        });
    }

    void *producer = zmq_socket(ctx, ZMQ_PUSH);
    zmq_setsockopt(producer, ZMQ_SNDHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(producer, ZMQ_SNDTIMEO, &timeo_ms, sizeof(timeo_ms));
    zmq_setsockopt(producer, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_connect(producer, url_a.c_str());

    std::thread prod([&]() {
        pin_this_thread(c.prod_cores);
        std::vector<char> payload(c.frame_bytes, 7);
        while (!stop.load(std::memory_order_relaxed)) {
            zmq_msg_t msg;
            zmq_msg_init_size(&msg, c.frame_bytes);
            std::memcpy(zmq_msg_data(&msg), payload.data(), c.frame_bytes);
            // Send is timeout-bounded; on EAGAIN we re-check stop and retry,
            // so shutdown never deadlocks on a full HWM.
            while (zmq_msg_send(&msg, producer, 0) < 0) {
                if (stop.load(std::memory_order_relaxed)) {
                    zmq_msg_close(&msg);
                    return;
                }
            }
        }
    });

    prod.join();
    if (c.relay)
        relay_thread.join();
    cons.join();

    zmq_close(producer);
    if (relay_in)
        zmq_close(relay_in);
    if (relay_out)
        zmq_close(relay_out);
    zmq_close(consumer);
    zmq_ctx_term(ctx);
    return received.load();
}

// Run K parallel pipelines for `secs`, return aggregate GB/s.
double run_pipelines(int k, PipelineCfg base, double secs,
                     const std::vector<std::vector<int>> &l3, int n_physical,
                     const std::string &pin_mode) {
    std::atomic<bool> stop{false};
    std::vector<uint64_t> results(k, 0);
    std::vector<std::thread> drivers;

    for (int i = 0; i < k; ++i) {
        drivers.emplace_back([&, i]() {
            PipelineCfg c = base;
            c.base_port = base.base_port + i * 4;

            // Pinning strategy.
            if (!l3.empty() && pin_mode != "none") {
                int ngrp = (int)l3.size();
                auto g = [&](int idx) {
                    return physical_only(l3[idx % ngrp], n_physical);
                };
                if (pin_mode == "samel3") {
                    auto grp = g(i);
                    c.prod_cores = c.cons_cores = c.relay_cores = grp;
                    c.ioctx_cores = grp;
                } else if (pin_mode == "splitl3") {
                    c.prod_cores = g(2 * i);
                    c.cons_cores = g(2 * i + 1);
                    c.relay_cores = g(2 * i);
                    c.ioctx_cores = g(2 * i);
                } else if (pin_mode == "spread") {
                    // round-robin physical cores
                    std::vector<int> all;
                    for (auto &grp : l3)
                        for (int cc : physical_only(grp, n_physical))
                            all.push_back(cc);
                    if (!all.empty()) {
                        c.prod_cores = {all[(3 * i) % all.size()]};
                        c.cons_cores = {all[(3 * i + 1) % all.size()]};
                        c.relay_cores = {all[(3 * i + 2) % all.size()]};
                    }
                }
            }
            results[i] = run_pipeline(c, stop);
        });
    }

    auto t0 = Clock::now();
    std::this_thread::sleep_for(std::chrono::duration<double>(secs));
    stop.store(true);
    for (auto &d : drivers)
        d.join();
    double elapsed = seconds_since(t0);

    uint64_t total = 0;
    for (auto r : results)
        total += r;
    return ((double)total / elapsed) / 1e9;
}

// ---------------------------------------------------------------------------
// 3. Two-node mode (source | relay | sink) over real NICs
// ---------------------------------------------------------------------------
//
// The loopback sweep above deliberately CANNOT separate the relay from its
// endpoints: producer, relay and consumer all share one DRAM bus, so a relay
// looks ~2x worse than it is in production where the sender is on a psana node
// and the receiver is at NERSC. These roles let you place each stage on its
// own host and measure the relay in isolation over a real network:
//
//   node C (final dest):  bench --sink            --sink-port 5600
//   node B (relay):       bench --relay-node      --relay-listen 5600 \
//                                                 --relay-connect C_HOST:5600
//   node A (source):      bench --source          --source-connect B_HOST:5600
//
// Data flows A --PUSH--> B(PULL->PUSH) --PUSH--> C(PULL). Downstream stages
// bind, upstream stages connect (so the source/relay dial outward, matching
// the "can only connect out" constraint for the WAN-facing leg).

// Set on SIGINT so roles with secs<=0 (run-forever) can stop cleanly.
std::atomic<bool> g_signal_stop{false};

// Explicit TCP socket buffer size in bytes (0 = leave kernel autotuning on).
// Set from --sockbuf to fill a high bandwidth-delay-product WAN path. NOTE:
// the kernel caps this at net.core.{r,w}mem_max and setting it disables
// receive autotuning, so it can hurt if smaller than what autotuning picks.
int g_sockbuf_bytes = 0;

void common_sockopts(void *s, int hwm, int timeo_ms, int linger) {
    zmq_setsockopt(s, ZMQ_SNDHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(s, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(s, ZMQ_SNDTIMEO, &timeo_ms, sizeof(timeo_ms));
    zmq_setsockopt(s, ZMQ_RCVTIMEO, &timeo_ms, sizeof(timeo_ms));
    zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof(linger));
    if (g_sockbuf_bytes > 0) {
        zmq_setsockopt(s, ZMQ_SNDBUF, &g_sockbuf_bytes, sizeof(g_sockbuf_bytes));
        zmq_setsockopt(s, ZMQ_RCVBUF, &g_sockbuf_bytes, sizeof(g_sockbuf_bytes));
    }
}

// "host:port" -> {host, port}. If no ':' present, default_port is used.
struct HostPort {
    std::string host;
    int port;
};
HostPort split_hostport(const std::string &s, int default_port) {
    auto pos = s.rfind(':');
    if (pos == std::string::npos)
        return {s, default_port};
    return {s.substr(0, pos), std::stoi(s.substr(pos + 1))};
}

// Drives timing for a multi-stream role: waits for the first byte (so idle
// startup before peers connect is excluded), prints a per-second rolling
// aggregate rate, and stops after `secs` (or on SIGINT). Sets `stop` so the
// worker threads exit. Returns {total_bytes_at_stop, elapsed_seconds}.
std::pair<uint64_t, double> monitor_throughput(std::atomic<uint64_t> &bytes,
                                               std::atomic<bool> &stop,
                                               double secs, bool print_rolling) {
    while (bytes.load(std::memory_order_relaxed) == 0) {
        if (g_signal_stop.load(std::memory_order_relaxed)) {
            stop.store(true, std::memory_order_relaxed);
            return {0, 0.0};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auto t0 = Clock::now();
    auto last = t0;
    uint64_t last_bytes = bytes.load(std::memory_order_relaxed);
    while (true) {
        if (g_signal_stop.load(std::memory_order_relaxed))
            break;
        if (secs > 0 && seconds_since(t0) >= secs)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (print_rolling && seconds_since(last) >= 1.0) {
            uint64_t now = bytes.load(std::memory_order_relaxed);
            double since = seconds_since(last);
            double gbs = ((double)(now - last_bytes) / since) / 1e9;
            std::cout << "  [" << (int)seconds_since(t0) << "s] " << gbs
                      << " GB/s (" << gbs * 8.0 << " Gb/s)\n"
                      << std::flush;
            last_bytes = now;
            last = Clock::now();
        }
    }
    double el = seconds_since(t0);
    stop.store(true, std::memory_order_relaxed);
    return {bytes.load(std::memory_order_relaxed), el};
}

void print_role_result(const char *label, uint64_t bytes, double el) {
    double gbs = el > 0 ? ((double)bytes / el) / 1e9 : 0.0;
    std::cout << label << ": " << (double)bytes / 1e9 << " GB in " << el
              << " s = " << gbs << " GB/s (" << gbs * 8.0 << " Gb/s)\n";
}

// A bounded pool of reusable frame buffers for a realistic zero-copy producer.
// Unlike the single-reused-buffer benchmark cheat, a pool models what a real
// producer (e.g. lclstreamer) must do: it may NOT overwrite a buffer that is
// still queued inside ZMQ. Each frame acquires a free buffer, fills it, and
// sends it zero-copy; libzmq hands the buffer back to the pool via the release
// callback once the bytes are on the wire. No malloc/free in steady state.
struct BufferPool {
    std::mutex m;
    std::condition_variable cv;
    std::vector<char *> free_list;
    std::vector<char *> all;

    BufferPool(int n, size_t frame_bytes) {
        all.reserve(n);
        free_list.reserve(n);
        for (int i = 0; i < n; ++i) {
            char *b = new char[frame_bytes];
            std::memset(b, 7, frame_bytes);
            all.push_back(b);
            free_list.push_back(b);
        }
    }
    ~BufferPool() {
        for (char *b : all)
            delete[] b;
    }
    // Block until a buffer is free (or stop). Returns nullptr on stop.
    char *acquire(std::atomic<bool> &stop) {
        std::unique_lock<std::mutex> lk(m);
        while (free_list.empty()) {
            if (stop.load(std::memory_order_relaxed))
                return nullptr;
            cv.wait_for(lk, std::chrono::milliseconds(20));
        }
        char *b = free_list.back();
        free_list.pop_back();
        return b;
    }
    void release(char *b) {
        {
            std::lock_guard<std::mutex> lk(m);
            free_list.push_back(b);
        }
        cv.notify_one();
    }
};

// libzmq zero-copy release callback: hint is the owning BufferPool.
static void pool_release(void *data, void *hint) {
    static_cast<BufferPool *>(hint)->release(static_cast<char *>(data));
}

int run_source(const std::string &connect_addr, size_t frame_bytes,
               int io_threads, int hwm, double secs, int streams,
               int pool_size) {
    void *ctx = zmq_ctx_new();
    zmq_ctx_set(ctx, ZMQ_IO_THREADS, io_threads);
    HostPort hp = split_hostport(connect_addr, 5600);

    std::vector<void *> socks(streams, nullptr);
    for (int i = 0; i < streams; ++i) {
        void *push = zmq_socket(ctx, ZMQ_PUSH);
        common_sockopts(push, hwm, 100, 0);
        std::string url =
            "tcp://" + hp.host + ":" + std::to_string(hp.port + i);
        if (zmq_connect(push, url.c_str()) != 0) {
            std::cerr << "source: connect " << url << " failed: "
                      << zmq_strerror(zmq_errno()) << "\n";
            return 1;
        }
        socks[i] = push;
    }
    std::cout << "source: " << streams << " PUSH stream(s) -> " << hp.host
              << ":" << hp.port << "+[0.." << (streams - 1) << "], frame "
              << frame_bytes / 1024 << " KiB, io_threads " << io_threads
              << ", hwm " << hwm
              << (pool_size > 0 ? ", pool " + std::to_string(pool_size) +
                                      " buf/stream"
                                : ", single-buf")
              << "\n";

    std::atomic<uint64_t> sent{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> ws;
    // Two producer modes (buffers owned out here and freed only AFTER
    // zmq_ctx_term, because a libzmq io-thread may still reference one for an
    // in-flight send when the app thread returns -- freeing earlier is a UAF):
    //   pool_size == 0: one reused buffer per stream (benchmark cheat; constant
    //     bytes, never rewritten -- minimum allocator/memory pressure).
    //   pool_size >  0: a bounded buffer pool per stream that recycles buffers
    //     as ZMQ releases them, plus a per-frame memset to model writing fresh
    //     data. This is the realistic production path (no per-frame malloc).
    std::vector<char *> payloads;
    std::vector<std::unique_ptr<BufferPool>> pools;
    if (pool_size == 0) {
        payloads.assign(streams, nullptr);
        for (int i = 0; i < streams; ++i) {
            payloads[i] = new char[frame_bytes];
            std::memset(payloads[i], 7, frame_bytes);
        }
    } else {
        pools.reserve(streams);
        for (int i = 0; i < streams; ++i)
            pools.push_back(
                std::make_unique<BufferPool>(pool_size, frame_bytes));
    }
    for (int i = 0; i < streams; ++i) {
        ws.emplace_back([&, i]() {
            void *push = socks[i];
            if (pool_size == 0) {
                // Zero-copy from a single reused buffer; no-op free so libzmq
                // never frees it. Safe only because the bytes are constant.
                char *payload = payloads[i];
                auto no_free = [](void *, void *) {};
                while (!stop.load(std::memory_order_relaxed)) {
                    zmq_msg_t msg;
                    zmq_msg_init_data(&msg, payload, frame_bytes, no_free,
                                      nullptr);
                    while (zmq_msg_send(&msg, push, 0) < 0) {
                        if (stop.load(std::memory_order_relaxed)) {
                            zmq_msg_close(&msg);
                            return;
                        }
                    }
                    sent.fetch_add(frame_bytes, std::memory_order_relaxed);
                }
            } else {
                // Realistic pool: acquire a free buffer, write fresh data,
                // send zero-copy; pool_release recycles it once ZMQ is done.
                BufferPool *pool = pools[i].get();
                while (!stop.load(std::memory_order_relaxed)) {
                    char *buf = pool->acquire(stop);
                    if (!buf)
                        return;
                    std::memset(buf, 7, frame_bytes);
                    zmq_msg_t msg;
                    zmq_msg_init_data(&msg, buf, frame_bytes, pool_release,
                                      pool);
                    while (zmq_msg_send(&msg, push, 0) < 0) {
                        if (stop.load(std::memory_order_relaxed)) {
                            // close() runs pool_release, recycling buf.
                            zmq_msg_close(&msg);
                            return;
                        }
                    }
                    sent.fetch_add(frame_bytes, std::memory_order_relaxed);
                }
            }
        });
    }

    auto [total, el] = monitor_throughput(sent, stop, secs, false);
    for (auto &w : ws)
        w.join();
    print_role_result("source", total, el);
    for (void *s : socks)
        zmq_close(s);
    zmq_ctx_term(ctx);
    // ctx_term has flushed/stopped the io-threads; now the buffers are safe.
    // (pools, if any, free their buffers when this function returns -- also
    // after ctx_term.)
    for (char *p : payloads)
        delete[] p;
    return 0;
}

int run_sink(bool bind_mode, const std::string &connect_addr, int bind_base,
             int io_threads, int hwm, double secs, int streams) {
    void *ctx = zmq_ctx_new();
    zmq_ctx_set(ctx, ZMQ_IO_THREADS, io_threads);
    HostPort hp = split_hostport(connect_addr, 5700);

    // Two wiring modes:
    //   connect (default): the sink dials the relay's backend (ZMQ relay
    //     binds). Matches a downstream like NERSC that can only connect out.
    //   bind (--sink-bind): the sink listens and the relay/proxy connects in.
    //     Needed when an HAProxy TCP relay is used, because HAProxy's backend
    //     connects OUT to its server (the sink), so the sink must bind.
    std::vector<void *> socks(streams, nullptr);
    for (int i = 0; i < streams; ++i) {
        void *pull = zmq_socket(ctx, ZMQ_PULL);
        common_sockopts(pull, hwm, 200, 0);
        if (bind_mode) {
            std::string url =
                "tcp://0.0.0.0:" + std::to_string(bind_base + i);
            if (zmq_bind(pull, url.c_str()) != 0) {
                std::cerr << "sink: bind " << url << " failed: "
                          << zmq_strerror(zmq_errno()) << "\n";
                return 1;
            }
        } else {
            std::string url =
                "tcp://" + hp.host + ":" + std::to_string(hp.port + i);
            if (zmq_connect(pull, url.c_str()) != 0) {
                std::cerr << "sink: connect " << url << " failed: "
                          << zmq_strerror(zmq_errno()) << "\n";
                return 1;
            }
        }
        socks[i] = pull;
    }
    if (bind_mode)
        std::cout << "sink: " << streams << " PULL bind(s) 0.0.0.0:"
                  << bind_base << "+[0.." << (streams - 1) << "]";
    else
        std::cout << "sink: " << streams << " PULL connect(s) -> " << hp.host
                  << ":" << hp.port << "+[0.." << (streams - 1) << "]";
    std::cout << ", io_threads " << io_threads << ", hwm " << hwm
              << (secs <= 0 ? " (run until Ctrl-C)\n" : "\n");

    std::atomic<uint64_t> recvd{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> ws;
    for (int i = 0; i < streams; ++i) {
        ws.emplace_back([&, i]() {
            void *pull = socks[i];
            while (!stop.load(std::memory_order_relaxed)) {
                zmq_msg_t msg;
                zmq_msg_init(&msg);
                int rc = zmq_msg_recv(&msg, pull, 0);
                if (rc >= 0)
                    recvd.fetch_add((uint64_t)zmq_msg_size(&msg),
                                    std::memory_order_relaxed);
                zmq_msg_close(&msg);
            }
        });
    }

    auto [total, el] = monitor_throughput(recvd, stop, secs, true);
    for (auto &w : ws)
        w.join();
    print_role_result("sink", total, el);
    for (void *s : socks)
        zmq_close(s);
    zmq_ctx_term(ctx);
    return 0;
}

int run_relay_node(int frontend_base, int backend_base, int io_threads,
                   int hwm, double secs, int streams) {
    void *ctx = zmq_ctx_new();
    zmq_ctx_set(ctx, ZMQ_IO_THREADS, io_threads);

    // The relay BINDS both ends: the frontend PULL (the source/iana connects
    // in) and the backend PUSH (the sink/NERSC connects in). Only this SLAC
    // node listens; both peers dial outward to it.
    std::vector<void *> ins(streams, nullptr), outs(streams, nullptr);
    for (int i = 0; i < streams; ++i) {
        void *pull = zmq_socket(ctx, ZMQ_PULL);
        void *push = zmq_socket(ctx, ZMQ_PUSH);
        common_sockopts(pull, hwm, 200, 0);
        common_sockopts(push, hwm, 200, 0);
        std::string in_url =
            "tcp://0.0.0.0:" + std::to_string(frontend_base + i);
        std::string out_url =
            "tcp://0.0.0.0:" + std::to_string(backend_base + i);
        if (zmq_bind(pull, in_url.c_str()) != 0) {
            std::cerr << "relay: frontend bind " << in_url << " failed: "
                      << zmq_strerror(zmq_errno()) << "\n";
            return 1;
        }
        if (zmq_bind(push, out_url.c_str()) != 0) {
            std::cerr << "relay: backend bind " << out_url << " failed: "
                      << zmq_strerror(zmq_errno()) << "\n";
            return 1;
        }
        ins[i] = pull;
        outs[i] = push;
    }
    std::cout << "relay: " << streams << " stream(s) PULL bind 0.0.0.0:"
              << frontend_base << "+[0.." << (streams - 1)
              << "] (frontend, source connects) -> PUSH bind 0.0.0.0:"
              << backend_base << "+[0.." << (streams - 1)
              << "] (backend, sink connects), io_threads " << io_threads
              << ", hwm " << hwm
              << (secs <= 0 ? " (run until Ctrl-C)\n" : "\n");

    std::atomic<uint64_t> moved{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> ws;
    for (int i = 0; i < streams; ++i) {
        ws.emplace_back([&, i]() {
            void *pull = ins[i];
            void *push = outs[i];
            while (!stop.load(std::memory_order_relaxed)) {
                zmq_msg_t msg;
                zmq_msg_init(&msg);
                int rc = zmq_msg_recv(&msg, pull, 0);
                if (rc >= 0) {
                    size_t n = zmq_msg_size(&msg);
                    // recv->send on the same msg is a zero-copy move
                    // (refcount), the same trick cacheserver uses; the only
                    // copies are the kernel recv and the kernel send.
                    bool consumed = false;
                    while (zmq_msg_send(&msg, push, 0) < 0) {
                        if (stop.load(std::memory_order_relaxed)) {
                            zmq_msg_close(&msg);
                            consumed = true;
                            break;
                        }
                    }
                    if (consumed)
                        return;
                    moved.fetch_add((uint64_t)n, std::memory_order_relaxed);
                } else {
                    zmq_msg_close(&msg);
                }
            }
        });
    }

    auto [total, el] = monitor_throughput(moved, stop, secs, true);
    for (auto &w : ws)
        w.join();
    print_role_result("relay", total, el);
    for (void *s : ins)
        zmq_close(s);
    for (void *s : outs)
        zmq_close(s);
    zmq_ctx_term(ctx);
    return 0;
}

// ---------------------------------------------------------------------------
// 3c. Decoupled relay: per stream, a receiver thread and a sender thread
// pass messages through a bounded queue, so recv overlaps send instead of
// ping-ponging (recv->send->recv) on one thread. This attacks the half-idle,
// serialization-bound behavior seen with run_relay_node: there the single
// per-stream thread blocks in send (waiting on the WAN egress credit) while
// not receiving, and blocks in recv while not sending. Here the two phases
// run concurrently and the queue absorbs RTT jitter on either leg.
namespace {
// Minimal bounded MPSC-ish queue of zmq_msg_t (single producer = receiver,
// single consumer = sender). Holds ownership of the message until popped.
struct MsgQueue {
    explicit MsgQueue(size_t cap) : cap_(cap) {}
    // Returns false if stopped while full.
    bool push(zmq_msg_t &m, std::atomic<bool> &stop) {
        std::unique_lock<std::mutex> lk(mu_);
        not_full_.wait(lk, [&] {
            return q_.size() < cap_ || stop.load(std::memory_order_relaxed);
        });
        if (stop.load(std::memory_order_relaxed))
            return false;
        q_.push_back(m); // shallow copy of the zmq_msg_t handle; ownership moves
        lk.unlock();
        not_empty_.notify_one();
        return true;
    }
    // Returns false if stopped and drained.
    bool pop(zmq_msg_t &out, std::atomic<bool> &stop) {
        std::unique_lock<std::mutex> lk(mu_);
        not_empty_.wait(lk, [&] {
            return !q_.empty() || stop.load(std::memory_order_relaxed);
        });
        if (q_.empty())
            return false;
        out = q_.front();
        q_.pop_front();
        lk.unlock();
        not_full_.notify_one();
        return true;
    }
    void wake_all() {
        not_full_.notify_all();
        not_empty_.notify_all();
    }
    // Close any messages still queued at shutdown.
    void drain_close() {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &m : q_)
            zmq_msg_close(&m);
        q_.clear();
    }

  private:
    std::mutex mu_;
    std::condition_variable not_full_, not_empty_;
    std::deque<zmq_msg_t> q_;
    size_t cap_;
};
} // namespace

int run_relay_async(int frontend_base, int backend_base, int io_threads,
                    int hwm, double secs, int streams, size_t queue_cap) {
    void *ctx = zmq_ctx_new();
    zmq_ctx_set(ctx, ZMQ_IO_THREADS, io_threads);

    std::vector<void *> ins(streams, nullptr), outs(streams, nullptr);
    for (int i = 0; i < streams; ++i) {
        void *pull = zmq_socket(ctx, ZMQ_PULL);
        void *push = zmq_socket(ctx, ZMQ_PUSH);
        common_sockopts(pull, hwm, 200, 0);
        common_sockopts(push, hwm, 200, 0);
        std::string in_url =
            "tcp://0.0.0.0:" + std::to_string(frontend_base + i);
        std::string out_url =
            "tcp://0.0.0.0:" + std::to_string(backend_base + i);
        if (zmq_bind(pull, in_url.c_str()) != 0) {
            std::cerr << "relay-async: frontend bind " << in_url
                      << " failed: " << zmq_strerror(zmq_errno()) << "\n";
            return 1;
        }
        if (zmq_bind(push, out_url.c_str()) != 0) {
            std::cerr << "relay-async: backend bind " << out_url
                      << " failed: " << zmq_strerror(zmq_errno()) << "\n";
            return 1;
        }
        ins[i] = pull;
        outs[i] = push;
    }
    std::cout << "relay-async: " << streams
              << " stream(s) PULL bind 0.0.0.0:" << frontend_base << "+[0.."
              << (streams - 1) << "] -> PUSH bind 0.0.0.0:" << backend_base
              << "+[0.." << (streams - 1)
              << "], decoupled recv/send, queue " << queue_cap << " msgs/stream"
              << ", io_threads " << io_threads << ", hwm " << hwm
              << (secs <= 0 ? " (run until Ctrl-C)\n" : "\n");

    std::atomic<uint64_t> moved{0};
    std::atomic<bool> stop{false};
    std::vector<std::unique_ptr<MsgQueue>> queues;
    for (int i = 0; i < streams; ++i)
        queues.emplace_back(std::make_unique<MsgQueue>(queue_cap));

    std::vector<std::thread> ws;
    for (int i = 0; i < streams; ++i) {
        // Receiver: PULL -> queue. Counts here so throughput reflects bytes
        // received (the sender drains them to the wire shortly after).
        ws.emplace_back([&, i]() {
            void *pull = ins[i];
            MsgQueue &q = *queues[i];
            while (!stop.load(std::memory_order_relaxed)) {
                zmq_msg_t msg;
                zmq_msg_init(&msg);
                int rc = zmq_msg_recv(&msg, pull, 0);
                if (rc < 0) {
                    zmq_msg_close(&msg);
                    continue; // timeout (RCVTIMEO); re-check stop
                }
                moved.fetch_add((uint64_t)zmq_msg_size(&msg),
                                std::memory_order_relaxed);
                if (!q.push(msg, stop)) {
                    zmq_msg_close(&msg); // stopped while full
                    return;
                }
            }
        });
        // Sender: queue -> PUSH.
        ws.emplace_back([&, i]() {
            void *push = outs[i];
            MsgQueue &q = *queues[i];
            while (true) {
                zmq_msg_t msg;
                if (!q.pop(msg, stop))
                    return; // stopped and drained
                while (zmq_msg_send(&msg, push, 0) < 0) {
                    if (stop.load(std::memory_order_relaxed)) {
                        zmq_msg_close(&msg);
                        return;
                    }
                }
            }
        });
    }

    auto [total, el] = monitor_throughput(moved, stop, secs, true);
    // Wake any threads blocked on queue full/empty so they can observe stop.
    for (auto &q : queues)
        q->wake_all();
    for (auto &w : ws)
        w.join();
    for (auto &q : queues)
        q->drain_close();
    print_role_result("relay-async", total, el);
    for (void *s : ins)
        zmq_close(s);
    for (void *s : outs)
        zmq_close(s);
    zmq_ctx_term(ctx);
    return 0;
}

// ---------------------------------------------------------------------------
// 3b. splice() relay (Linux) — binds BOTH sides, zero userspace copies
// ---------------------------------------------------------------------------
//
// A reverse proxy (HAProxy/nginx) cannot satisfy "the middle binds and BOTH
// ends connect in", because their backend always dials out. This relay does
// what HAProxy does internally (kernel splice() zero-copy forwarding) while
// binding both the frontend and the backend, so iana and NERSC can both
// connect inward.
//
// It is transparent at the byte level: ZMQ's ZMTP greeting and frames flow
// through untouched, so a ZMQ PUSH (source) and PULL (sink) negotiate end to
// end as if directly connected. Stream i pairs frontend port (listen_base+i)
// with backend port (backend_base+i).
#if defined(__linux__)
static int tcp_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (g_sockbuf_bytes > 0) {
        // Set before listen() so accepted sockets inherit the (receive) size.
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &g_sockbuf_bytes,
                   sizeof(g_sockbuf_bytes));
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &g_sockbuf_bytes,
                   sizeof(g_sockbuf_bytes));
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(fd, (sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 1) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Move bytes src->dst with zero userspace copies via a kernel pipe. Counts
// forwarded bytes (when counter != nullptr). Returns when the peer closes or
// `stop` is set (the fd is shut down from the main thread to unblock splice).
static void splice_loop(int src, int dst, std::atomic<uint64_t> *counter,
                        std::atomic<bool> &stop) {
    int p[2];
    if (pipe(p) != 0)
        return;
    const size_t CHUNK = 1 << 20; // 1 MiB per splice
    while (!stop.load(std::memory_order_relaxed)) {
        ssize_t n = splice(src, nullptr, p[1], nullptr, CHUNK,
                           SPLICE_F_MOVE | SPLICE_F_MORE);
        if (n <= 0)
            break; // peer closed or error
        ssize_t left = n;
        while (left > 0) {
            ssize_t m = splice(p[0], nullptr, dst, nullptr, (size_t)left,
                               SPLICE_F_MOVE | SPLICE_F_MORE);
            if (m <= 0)
                goto out;
            left -= m;
            if (counter)
                counter->fetch_add((uint64_t)m, std::memory_order_relaxed);
        }
    }
out:
    close(p[0]);
    close(p[1]);
}
#endif

int run_splice_relay(int listen_base, int backend_base, double secs,
                     int streams) {
#if !defined(__linux__)
    (void)listen_base;
    (void)backend_base;
    (void)secs;
    (void)streams;
    std::cerr << "splice-relay: only supported on Linux\n";
    return 2;
#else
    std::vector<int> front_listen(streams, -1), back_listen(streams, -1);
    for (int i = 0; i < streams; ++i) {
        front_listen[i] = tcp_listen(listen_base + i);
        back_listen[i] = tcp_listen(backend_base + i);
        if (front_listen[i] < 0 || back_listen[i] < 0) {
            std::cerr << "splice-relay: listen failed on stream " << i << "\n";
            return 1;
        }
    }
    std::cout << "splice-relay: " << streams << " stream(s) accept frontend "
              << "0.0.0.0:" << listen_base << "+[0.." << (streams - 1)
              << "] (source connects) + backend 0.0.0.0:" << backend_base
              << "+[0.." << (streams - 1) << "] (sink connects), splice() "
              << "zero-copy"
              << (secs <= 0 ? " (run until Ctrl-C)\n" : "\n");
    std::cout << "  waiting for both ends to connect on each stream...\n"
              << std::flush;

    // Accept one source and one sink per stream. Accept blocks until both
    // peers have dialed in, so order of starting source/sink does not matter.
    std::vector<int> fsock(streams, -1), bsock(streams, -1);
    for (int i = 0; i < streams; ++i) {
        fsock[i] = accept(front_listen[i], nullptr, nullptr);
        bsock[i] = accept(back_listen[i], nullptr, nullptr);
        if (fsock[i] < 0 || bsock[i] < 0) {
            std::cerr << "splice-relay: accept failed on stream " << i << "\n";
            return 1;
        }
        int one = 1;
        setsockopt(fsock[i], IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        setsockopt(bsock[i], IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        if (g_sockbuf_bytes > 0) {
            for (int fd : {fsock[i], bsock[i]}) {
                setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &g_sockbuf_bytes,
                           sizeof(g_sockbuf_bytes));
                setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &g_sockbuf_bytes,
                           sizeof(g_sockbuf_bytes));
            }
        }
    }

    std::atomic<uint64_t> moved{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> ts;
    for (int i = 0; i < streams; ++i) {
        // Forward (source->sink): the measured data path.
        ts.emplace_back([&, i]() { splice_loop(fsock[i], bsock[i], &moved, stop); });
        // Reverse (sink->source): carries ZMTP greeting/back-chatter; not
        // counted, but required so the end-to-end handshake completes.
        ts.emplace_back([&, i]() { splice_loop(bsock[i], fsock[i], nullptr, stop); });
    }

    auto [total, el] = monitor_throughput(moved, stop, secs, true);
    // Shut the sockets so any blocked splice() returns and threads can join.
    for (int i = 0; i < streams; ++i) {
        shutdown(fsock[i], SHUT_RDWR);
        shutdown(bsock[i], SHUT_RDWR);
    }
    for (auto &t : ts)
        t.join();
    print_role_result("splice-relay", total, el);
    for (int i = 0; i < streams; ++i) {
        close(fsock[i]);
        close(bsock[i]);
        close(front_listen[i]);
        close(back_listen[i]);
    }
    return 0;
#endif
}

// ---------------------------------------------------------------------------
// MSG_ZEROCOPY relay (Linux): forward with a zero-copy TX path
// ---------------------------------------------------------------------------
// Unlike splice (which keeps bytes entirely in-kernel), this relay does a
// normal recv() into a userspace buffer but sends with MSG_ZEROCOPY, so the
// kernel DMAs straight from our buffer to the NIC with no send-side copy. It
// stays ordinary TCP on the wire (WAN-safe, unlike RDMA/RoCE). The cost is
// that a sent buffer must NOT be reused until the kernel signals completion
// on the socket error queue (MSG_ERRQUEUE), so we manage a buffer pool keyed
// by the kernel's per-socket completion counter.
#if defined(__linux__)
#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY 60
#endif
#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY 0x4000000
#endif
#ifndef SO_EE_ORIGIN_ZEROCOPY
#define SO_EE_ORIGIN_ZEROCOPY 5
#endif
#ifndef SO_EE_CODE_ZEROCOPY_COPIED
#define SO_EE_CODE_ZEROCOPY_COPIED 1
#endif
// These are standard Linux constants but are not always visible depending on
// which headers/feature-test macros are in scope; provide fallbacks.
#ifndef SOL_IP
#define SOL_IP 0
#endif
#ifndef SOL_IPV6
#define SOL_IPV6 41
#endif
#ifndef IP_RECVERR
#define IP_RECVERR 11
#endif
#ifndef IPV6_RECVERR
#define IPV6_RECVERR 25
#endif

// Drain MSG_ERRQUEUE completions. Each completed send id decrements the owning
// buffer's refcount; a buffer returns to the free list when its refcount hits
// 0. Returns completions reaped; sets *copied if the kernel fell back to a copy
// (i.e. zero-copy did not actually engage for some sends).
static int zc_drain(int fd, std::vector<int> &refcount,
                    std::vector<int> &freelist,
                    std::unordered_map<uint32_t, int> &id_to_buf,
                    bool *copied) {
    int reaped = 0;
    while (true) {
        char ctrl[256];
        msghdr msg{};
        msg.msg_control = ctrl;
        msg.msg_controllen = sizeof(ctrl);
        ssize_t r = recvmsg(fd, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);
        if (r < 0)
            break; // EAGAIN: error queue drained
        for (cmsghdr *cm = CMSG_FIRSTHDR(&msg); cm;
             cm = CMSG_NXTHDR(&msg, cm)) {
            bool is_err = (cm->cmsg_level == SOL_IP &&
                           cm->cmsg_type == IP_RECVERR) ||
                          (cm->cmsg_level == SOL_IPV6 &&
                           cm->cmsg_type == IPV6_RECVERR);
            if (!is_err)
                continue;
            auto *serr = (sock_extended_err *)CMSG_DATA(cm);
            if (serr->ee_origin != SO_EE_ORIGIN_ZEROCOPY)
                continue;
            if ((serr->ee_code & SO_EE_CODE_ZEROCOPY_COPIED) && copied)
                *copied = true;
            uint32_t lo = serr->ee_info, hi = serr->ee_data;
            for (uint32_t id = lo;; ++id) {
                auto it = id_to_buf.find(id);
                if (it != id_to_buf.end()) {
                    int b = it->second;
                    id_to_buf.erase(it);
                    if (--refcount[b] == 0)
                        freelist.push_back(b);
                    ++reaped;
                }
                if (id == hi)
                    break; // inclusive range; guard against wrap
            }
        }
    }
    return reaped;
}

// Forward src->dst using MSG_ZEROCOPY on dst. counter (if set) accumulates
// forwarded bytes. Returns when the peer closes or stop is set.
static void zerocopy_loop(int src, int dst, std::atomic<uint64_t> *counter,
                          std::atomic<bool> &stop, std::atomic<bool> *fellback) {
    const int N = 32;             // buffer pool depth (max sends in flight)
    const size_t CHUNK = 1 << 20; // 1 MiB per recv/send
    int one = 1;
    bool zc = setsockopt(dst, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one)) == 0;
    std::vector<char *> bufs(N);
    for (auto &b : bufs)
        b = new char[CHUNK];
    std::vector<int> refcount(N, 0);
    std::vector<int> freelist;
    freelist.reserve(N);
    for (int i = N - 1; i >= 0; --i)
        freelist.push_back(i);
    std::unordered_map<uint32_t, int> id_to_buf;
    uint32_t next_id = 0; // matches the kernel's per-socket completion counter

    while (!stop.load(std::memory_order_relaxed)) {
        // Need a free buffer; drain completions (or briefly wait) until one is.
        while (freelist.empty()) {
            bool copied = false;
            int got = zc_drain(dst, refcount, freelist, id_to_buf, &copied);
            if (copied && fellback)
                fellback->store(true, std::memory_order_relaxed);
            if (got == 0) {
                if (stop.load(std::memory_order_relaxed))
                    goto out;
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }
        int b = freelist.back();
        freelist.pop_back();
        ssize_t n = recv(src, bufs[b], CHUNK, 0);
        if (n <= 0) {
            freelist.push_back(b);
            break; // peer closed or error
        }
        size_t off = 0;
        while (off < (size_t)n) {
            ssize_t m = send(dst, bufs[b] + off, (size_t)n - off,
                             zc ? MSG_ZEROCOPY : 0);
            if (m < 0) {
                if (errno == ENOBUFS || errno == EAGAIN) {
                    // Too many pinned pages in flight; reap and retry.
                    zc_drain(dst, refcount, freelist, id_to_buf, nullptr);
                    if (stop.load(std::memory_order_relaxed))
                        goto out;
                    continue;
                }
                goto out;
            }
            if (zc) {
                // This send() consumed completion id next_id; pin the buffer
                // until that id is reported done on the error queue.
                id_to_buf[next_id++] = b;
                ++refcount[b];
            }
            off += (size_t)m;
            if (counter)
                counter->fetch_add((uint64_t)m, std::memory_order_relaxed);
        }
        if (!zc)
            freelist.push_back(b); // no completion will arrive; reuse now
    }
out:
    // Best-effort: let outstanding completions land so pages unpin before free.
    for (int k = 0; k < 200 && !id_to_buf.empty(); ++k) {
        if (zc_drain(dst, refcount, freelist, id_to_buf, nullptr) == 0)
            std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    for (auto *b : bufs)
        delete[] b;
}

int run_zerocopy_relay(int listen_base, int backend_base, double secs,
                       int streams) {
    std::vector<int> front_listen(streams, -1), back_listen(streams, -1);
    for (int i = 0; i < streams; ++i) {
        front_listen[i] = tcp_listen(listen_base + i);
        back_listen[i] = tcp_listen(backend_base + i);
        if (front_listen[i] < 0 || back_listen[i] < 0) {
            std::cerr << "zerocopy-relay: listen failed on stream " << i
                      << "\n";
            return 1;
        }
    }
    std::cout << "zerocopy-relay: " << streams << " stream(s) accept frontend "
              << "0.0.0.0:" << listen_base << "+[0.." << (streams - 1)
              << "] (source connects) + backend 0.0.0.0:" << backend_base
              << "+[0.." << (streams - 1) << "] (sink connects), "
              << "recv()+send(MSG_ZEROCOPY)"
              << (secs <= 0 ? " (run until Ctrl-C)\n" : "\n");
    std::cout << "  waiting for both ends to connect on each stream...\n"
              << std::flush;

    std::vector<int> fsock(streams, -1), bsock(streams, -1);
    for (int i = 0; i < streams; ++i) {
        fsock[i] = accept(front_listen[i], nullptr, nullptr);
        bsock[i] = accept(back_listen[i], nullptr, nullptr);
        if (fsock[i] < 0 || bsock[i] < 0) {
            std::cerr << "zerocopy-relay: accept failed on stream " << i
                      << "\n";
            return 1;
        }
        int one = 1;
        setsockopt(fsock[i], IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        setsockopt(bsock[i], IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        if (g_sockbuf_bytes > 0) {
            for (int fd : {fsock[i], bsock[i]}) {
                setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &g_sockbuf_bytes,
                           sizeof(g_sockbuf_bytes));
                setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &g_sockbuf_bytes,
                           sizeof(g_sockbuf_bytes));
            }
        }
    }

    std::atomic<uint64_t> moved{0};
    std::atomic<bool> stop{false};
    std::atomic<bool> fellback{false};
    std::vector<std::thread> ts;
    for (int i = 0; i < streams; ++i) {
        // Forward (source->sink) with MSG_ZEROCOPY: the measured path.
        ts.emplace_back([&, i]() {
            zerocopy_loop(fsock[i], bsock[i], &moved, stop, &fellback);
        });
        // Reverse (sink->source) ZMTP back-chatter via plain splice; uncounted.
        ts.emplace_back(
            [&, i]() { splice_loop(bsock[i], fsock[i], nullptr, stop); });
    }

    auto [total, el] = monitor_throughput(moved, stop, secs, true);
    for (int i = 0; i < streams; ++i) {
        shutdown(fsock[i], SHUT_RDWR);
        shutdown(bsock[i], SHUT_RDWR);
    }
    for (auto &t : ts)
        t.join();
    print_role_result("zerocopy-relay", total, el);
    if (fellback.load(std::memory_order_relaxed))
        std::cout << "  NOTE: kernel fell back to COPY on some sends "
                     "(zero-copy did not fully engage; frames may be < ~10 KiB "
                     "or the NIC/driver lacks SG support)\n";
    for (int i = 0; i < streams; ++i) {
        close(fsock[i]);
        close(bsock[i]);
        close(front_listen[i]);
        close(back_listen[i]);
    }
    return 0;
}
#else
int run_zerocopy_relay(int, int, double, int) {
    std::cerr << "zerocopy-relay: only supported on Linux\n";
    return 2;
}
#endif

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
struct Args {
    size_t frame_kb = 4096; // 4 MiB default frame
    int io_threads = 1;
    int hwm = 4;
    double secs = 3.0;
    int max_pipelines = 8;
    bool relay = false;
    std::string pin = "spread"; // none|samel3|splitl3|spread
    int base_port = 5600;
    bool suite = true;

    // Two-node mode. role is one of: "" (local suite), "source", "sink",
    // "relay-node".
    //
    // Topology (only the relay binds; both ends connect outward):
    //   source --PUSH connect--> relay frontend (bind)
    //   relay  --PUSH bind------> sink (PULL connect)
    // so a downstream that cannot bind (e.g. NERSC) still works.
    std::string role;
    std::string connect_addr;  // source: relay frontend; sink: relay backend
    int listen_port = 5600;    // relay frontend bind port (source connects)
    int backend_port = 5700;   // relay backend bind port (sink connects)
    int streams = 1;           // parallel PUSH/PULL connections (ports base+i)
    bool sink_bind = false;    // sink binds instead of connects (HAProxy relay)
    int sink_bind_port = 5700; // base bind port when sink_bind is set
    int sockbuf_mb = 0;        // TCP socket buffer MiB (0 = kernel autotune)
    int relay_queue = 256;     // relay-async: queued msgs per stream
    int source_pool = 0;       // source: buffers/stream (0 = single reused buf)
};

Args parse(int argc, char **argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        auto next = [&]() { return std::string(argv[++i]); };
        if (s == "--frame-kb")
            a.frame_kb = std::stoul(next());
        else if (s == "--io-threads")
            a.io_threads = std::stoi(next());
        else if (s == "--hwm")
            a.hwm = std::stoi(next());
        else if (s == "--secs")
            a.secs = std::stod(next());
        else if (s == "--max-pipelines")
            a.max_pipelines = std::stoi(next());
        else if (s == "--relay")
            a.relay = true;
        else if (s == "--pin")
            a.pin = next();
        else if (s == "--base-port")
            a.base_port = std::stoi(next());
        else if (s == "--single")
            a.suite = false;
        else if (s == "--source")
            a.role = "source";
        else if (s == "--sink")
            a.role = "sink";
        else if (s == "--relay-node")
            a.role = "relay-node";
        else if (s == "--relay-async")
            a.role = "relay-async";
        else if (s == "--relay-queue")
            a.relay_queue = std::stoi(next());
        else if (s == "--splice-relay")
            a.role = "splice-relay";
        else if (s == "--zerocopy-relay")
            a.role = "zerocopy-relay";
        else if (s == "--source-connect" || s == "--sink-connect")
            a.connect_addr = next();
        else if (s == "--relay-listen")
            a.listen_port = std::stoi(next());
        else if (s == "--relay-backend")
            a.backend_port = std::stoi(next());
        else if (s == "--sink-bind") {
            a.sink_bind = true;
            a.sink_bind_port = std::stoi(next());
        }
        else if (s == "--streams")
            a.streams = std::stoi(next());
        else if (s == "--sockbuf")
            a.sockbuf_mb = std::stoi(next());
        else if (s == "--source-pool")
            a.source_pool = std::stoi(next());
        else if (s == "--help") {
            std::cout
                << "Usage: bench [opts]\n"
                << "  --frame-kb N      frame size in KiB (default 4096)\n"
                << "  --io-threads N    ZMQ_IO_THREADS per context (default 1)\n"
                << "  --hwm N           send/recv HWM (default 4)\n"
                << "  --secs S          seconds per measurement, 0=until "
                   "Ctrl-C (default 3)\n"
                << "  --max-pipelines N max parallel pipelines in scaling "
                   "sweep\n"
                << "  --relay           insert PULL->PUSH relay hop "
                   "(local suite)\n"
                << "  --pin MODE        none|samel3|splitl3|spread\n"
                << "  --single          run one config instead of full "
                   "suite\n"
                << "\n two-node mode (ONLY the relay binds; both ends "
                   "connect out):\n"
                << "  --relay-node --relay-listen N --relay-backend M\n"
                << "        binds PULL on N..N+K-1 (source connects) and\n"
                << "        PUSH on M..M+K-1 (sink connects)\n"
                << "  --relay-async --relay-listen N --relay-backend M\n"
                << "        like --relay-node but DECOUPLES recv/send per "
                   "stream\n"
                << "        (receiver + sender thread + queue) so recv "
                   "overlaps send.\n"
                << "        --relay-queue Q   queued msgs/stream (default "
                   "256)\n"
                << "  --splice-relay --relay-listen N --relay-backend M\n"
                << "        Linux kernel splice() zero-copy relay; binds "
                   "BOTH\n"
                << "        ends (transparent to ZMQ). Same source/sink "
                   "flags.\n"
                << "  --zerocopy-relay --relay-listen N --relay-backend M\n"
                << "        Linux recv()+send(MSG_ZEROCOPY) relay; binds "
                   "BOTH\n"
                << "        ends. Zero-copy TX over ordinary TCP (WAN-safe, "
                   "unlike\n"
                << "        RDMA). Warns if the kernel falls back to a copy.\n"
                << "  --source --source-connect RELAY_HOST:N\n"
                << "  --source-pool N   source uses an N-buffer recycling pool\n"
                << "                    per stream (+per-frame memset) instead\n"
                << "                    of the single reused buffer; models a\n"
                << "                    real producer. 0 = single buf (default)\n"
                << "  --sink   --sink-connect   RELAY_HOST:M\n"
                << "  --sink   --sink-bind      M   (sink LISTENS; for an "
                   "HAProxy relay whose backend connects out)\n"
                << "  --streams K       K parallel connections (ports "
                   "base..base+K-1)\n"
                << "                    (all three roles must use the same "
                   "K)\n"
                << "  --sockbuf MB      TCP SO_SND/RCVBUF (+ZMQ_SND/RCVBUF) "
                   "per stream;\n"
                << "                    fills WAN BDP. Capped by "
                   "net.core.{r,w}mem_max.\n"
                << "  e.g.  bench --relay-node --relay-listen 5600 "
                   "--relay-backend 5700 --secs 0 --streams 8\n"
                << "        bench --source --source-connect relayhost:5600 "
                   "--secs 20 --streams 8\n"
                << "        bench --sink   --sink-connect   relayhost:5700 "
                   "--secs 0 --streams 8\n";
            std::exit(0);
        }
    }
    return a;
}

} // namespace

int main(int argc, char **argv) {
    Args a = parse(argc, argv);

    int major, minor, patch;
    zmq_version(&major, &minor, &patch);
    std::cout << "libzmq " << major << "." << minor << "." << patch << "\n";

    if (a.sockbuf_mb > 0)
        g_sockbuf_bytes = a.sockbuf_mb * 1024 * 1024;

    // Two-node roles short-circuit the local DRAM/loopback suite: they speak
    // to other hosts over real NICs instead.
    if (!a.role.empty()) {
        std::signal(SIGINT, [](int) {
            g_signal_stop.store(true, std::memory_order_relaxed);
        });
        const size_t fb = a.frame_kb * 1024;
        if (a.role == "source") {
            if (a.connect_addr.empty()) {
                std::cerr << "--source requires --source-connect HOST:PORT "
                             "(the relay frontend)\n";
                return 2;
            }
            return run_source(a.connect_addr, fb, a.io_threads, a.hwm, a.secs,
                              a.streams, a.source_pool);
        }
        if (a.role == "sink") {
            if (!a.sink_bind && a.connect_addr.empty()) {
                std::cerr << "--sink requires --sink-connect HOST:PORT "
                             "(relay backend) or --sink-bind PORT (HAProxy)\n";
                return 2;
            }
            return run_sink(a.sink_bind, a.connect_addr, a.sink_bind_port,
                            a.io_threads, a.hwm, a.secs, a.streams);
        }
        if (a.role == "relay-node") {
            return run_relay_node(a.listen_port, a.backend_port, a.io_threads,
                                  a.hwm, a.secs, a.streams);
        }
        if (a.role == "relay-async") {
            return run_relay_async(a.listen_port, a.backend_port, a.io_threads,
                                   a.hwm, a.secs, a.streams,
                                   (size_t)a.relay_queue);
        }
        if (a.role == "splice-relay") {
            return run_splice_relay(a.listen_port, a.backend_port, a.secs,
                                    a.streams);
        }
        if (a.role == "zerocopy-relay") {
            return run_zerocopy_relay(a.listen_port, a.backend_port, a.secs,
                                      a.streams);
        }
        std::cerr << "unknown role: " << a.role << "\n";
        return 2;
    }

    unsigned hw = std::thread::hardware_concurrency();
    auto l3 = detect_l3_groups();
    // Heuristic: physical core count = half of logical if SMT, else logical.
    int n_physical = (int)hw;
    if (!l3.empty()) {
        // If groups contain ids >= hw/2, assume SMT and physical = hw/2.
        n_physical = (int)hw / 2;
        if (n_physical < 1)
            n_physical = (int)hw;
    }

    std::cout << "logical cpus: " << hw << ", assumed physical: " << n_physical
              << ", L3 groups: " << l3.size() << "\n";

    // Restrict everything to the cores this process may actually use. On a
    // cgroup-confined DTN this prevents libzmq from aborting when it tries to
    // pin an io-thread to a forbidden core, and keeps our own pinning honest.
    auto allowed = allowed_cpus();
    if (!allowed.empty()) {
        std::cout << "allowed cpus (" << allowed.size() << "):";
        for (int c : allowed)
            std::cout << " " << c;
        std::cout << "\n";
        size_t before = l3.size();
        l3 = filter_groups_to_allowed(l3, allowed);
        if (l3.size() != before)
            std::cout << "  (L3 groups reduced " << before << " -> " << l3.size()
                      << " after cpuset filtering)\n";
        if ((int)allowed.size() < n_physical) {
            std::cout << "  NOTE: process confined to " << allowed.size()
                      << " cpus; multi-L3 spread experiments are limited.\n";
        }
    }

    for (size_t i = 0; i < l3.size(); ++i) {
        std::cout << "  L3[" << i << "]:";
        for (int c : l3[i])
            std::cout << " " << c;
        std::cout << "\n";
    }
    std::cout << "\n";

    const size_t frame_bytes = a.frame_kb * 1024;

    // ---- 1. memcpy ceiling ----
    std::cout << "== DRAM bandwidth calibration (memcpy) ==\n";
    const size_t calib_buf = 256ull * 1024 * 1024; // 256 MiB > any L3
    double prev = 0;
    int ceil_threads = std::max(1, n_physical);
    for (int t : {1, 2, 4, 8, 16, ceil_threads}) {
        if (t > n_physical)
            continue;
        double gb = memcpy_bandwidth_gbps(t, calib_buf, 1.5, l3, n_physical);
        std::cout << "  threads=" << t << "  " << gb << " GB/s\n";
        prev = std::max(prev, gb);
    }
    double dram_ceiling = prev;
    std::cout << "  -> usable DRAM ceiling ~ " << dram_ceiling << " GB/s\n\n";

    PipelineCfg base;
    base.frame_bytes = frame_bytes;
    base.io_threads = a.io_threads;
    base.hwm = a.hwm;
    base.secs = a.secs;
    base.relay = a.relay;
    base.base_port = a.base_port;

    if (!a.suite) {
        double gb = run_pipelines(1, base, a.secs, l3, n_physical, a.pin);
        std::cout << "single pipeline: " << gb << " GB/s (frame "
                  << a.frame_kb << " KiB, io_threads " << a.io_threads
                  << ", relay " << (a.relay ? "on" : "off") << ", pin " << a.pin
                  << ")\n";
        return 0;
    }

    // ---- 2. ZMQ loopback scaling sweep ----
    std::cout << "== ZMQ TCP-loopback throughput (frame " << a.frame_kb
              << " KiB, io_threads " << a.io_threads << ", relay "
              << (a.relay ? "on" : "off") << ") ==\n";
    std::cout << "pipelines |   none   | samel3  | splitl3 | spread\n";
    std::cout << "----------+----------+---------+---------+--------\n";
    for (int k = 1; k <= a.max_pipelines; k *= 2) {
        std::cout << "   " << k << "\t  |";
        for (const char *mode : {"none", "samel3", "splitl3", "spread"}) {
            double gb = run_pipelines(k, base, a.secs, l3, n_physical, mode);
            std::cout << "  " << gb << "  |";
        }
        std::cout << "\n";
    }

    std::cout << "\n== verdict ==\n"
              << "Compare the best ZMQ aggregate against DRAM ceiling/2 ("
              << dram_ceiling / 2.0 << " GB/s).\n"
              << "  near ceiling/2  -> BANDWIDTH bound (need fewer copies: "
                 "splice/kernel-bypass)\n"
              << "  far below       -> CONTENTION bound (tune io_threads, "
                 "pinning, drop relay hop)\n"
              << "Also: if a pin mode beats 'none' substantially, scheduler "
                 "placement was hurting you.\n";
    return 0;
}
