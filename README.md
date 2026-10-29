# fastcache

Multi threaded cache that runs on the data transfer nodes to stream data to external facilities.

## Dependencies

- [vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started-vscode?pivots=shell-bash) — manages C++ deps

- cmake, ninja, pkg-config

## Build

Set `GRPC_PATH` and `EJFAT_PATH`env variables to gRPC and EJFat paths.

```sh

cmake  --preset  release

cmake  --build  --preset  release

```

### Build with conda (on s3df) 

```sh
source setup_env.sh

make
```

## Run

```sh

./build/lclstream-fastcache <config> # default: config/default.json

```

Config keys: `inurl`, `outurl`, `num_workers`, `io_threads`, `hwm`.

## Docker

Build it with:
```sh
podman-hpc build --network=slirp4netns -t fastcache_ejfat:1.0 .
```
Then run:
```sh
podman-hpc run --rm --env EJFAT_URI="<REDACTED>" --network host -v <config_location>:/usr/local/bin/default.json localhost/fastcache_ejfat:1.0 /usr/local/bin/lclstream-fastcache /usr/local/bin/default.json
```

## Config

This section describes the meaning of each field in the provided configuration.

#### General config

`inurl`: Example: ```tcp://localhost:5001```

Incoming ZeroMQ URL where the process receives messages to. Typically a tcp:// address with the node's IP where the fastcache is running.

`outurl`: (optional, default: `""`) Example:  ```tcp://localhost:5556```

Outgoing ZMQ URL where the receivers can connect to.

`type`: (optional, default: `LockFreePush`)   Application mode type. Controls **runtime pipeline behavior**

- `Simple`: Simple forward
- `Proxy`: zmq_proxy forward
- `BindInproc`: bind inproc forward
- `ConnectInproc`: bind connect forward
- `LockFreePush`: lock-free queue forward with two threads, this is the **default** mode.
- `LockFreeDealer`: lock-free router forward
- `LockFreeReply`: lock-free rep forward
- `TestOut`: simulated data for outgoing connection testing
- `TestIn`: mode for testing incoming connection speed (no queue)
- `ZMQPULLinEJFatOut`: lock-free EJFat forward with data from lclstreamer
- `TestInEJFatOut`: lock-free EJFat forward with simulated data
- `EJFatInZMQPUSHOut`: lock-free EJFat in, ZMQ PUSH out
- `EJFatInZMQREPOut`: lock-free EJFat in, ZMQ REPLY out
- `EJFatInZMQROUTEROut`: lock-free EJFat in, ZMQ ROUTER out

`helper_threads`: (optional, default: `0`) Number of extra worker threads used (not used in default mode)

`io_threads`: (optional, default: `16`) Number of ZMQ bakcground i/o threads in the main context.

`hwm`: (optional, default: `10`) High water mark, limits queued messages.

`timeout`: (optional, default: `-1`) Set in milliseconds. To block forever set to -1. Only counted once the receiver thread started working.

`simdata_size_mb`: (optional, default: `32`) Size of simulated data in MB (mode `TestOut` only)

`datasim_threads`: (optional, default: `1`) Number of workers that simulate data (mode `TestOut` only)

`cache_id`: Unique identifier for the ipc metrics url.

`verbose`: (optional, default: `false`) Enables logging of queue size.

#### Metrics config

`metrics`: (optional, default: `false`) Enables metrics to be sent via an ipc socket. Available only for type 4. IPC socket address is "ipc:///tmp/fastcache-metrics-receiver(/sender)-12345" (number is the cache id)

`ejfat_metrics_url`: (default: `""`) Must be set if metrics is enabled. URL to send metrics to.

`metrics_interval`: (optional, default: `10000`) Number of messages before metrics are sent to the ipc socket.

`cache_id`: (optional, default: `1`) Unique identifier for the ipc metrics url

#### EJFat config

`uselb`: (optional, default: `true` Sender/Receiver) Use load balancer (LB) with EJFat output.

`useipv6`: (optional, default: `false` Sender/Receiver) Use IPv6 and not IPv4.

`mtu`: (optional, default: `9000` Sender) MTU size, 9000 default.

`socketbufsize`: (optional, default: `16777216` Sender/Receiver) Buffer size for the socket (kernel doubles it, best to have wmem_max).

`rate_gbps`: (optional, default: `-1.0` Sender) Limit the send rate in Gbit/s.

`num_sendsockets`: (optional, default: `8` Sender) Number of send sockets per thread.

`data_id`: (optional, default: `1` Sender) EJFat data source ID number. Use different with multiple senders.

`queueSize`: (optional, default: `40` Sender) EJFat event queue size (smaller is better if the events are large).

`reportstats`: (optional, default: `true` Receiver) Report receiver stats back to LB .

`recvthreads`: (optional, default: `1` Receiver) Number of EJFat reassembling threads.

`eventtimeoutms`: (optional, default: `500` Receiver) Timeout for event reassembly in ms.

`period_ms`: (optional, default: `1000` Receiver) Period of the send state thread (ms).

`starting_port`: (optional, default: `10000` Receiver) Starting port number on which we are listening.

`ejfat_uri`: (required, no default value, Sender/Receiver) EJFat instance token URI to use for this session. Must be set in the environment as `EJFAT_URI`.

<!-- `dataSimulatorThreads`: Number of data simulator worker threads launched. -->
<!-- `ejfat_readthreads`: Number of EJFat read threads. -->

