# fastcache

Multi threaded cache that runs on the data transfer nodes to stream data to external facilities.

## Dependencies

- [vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started-vscode?pivots=shell-bash) — manages C++ deps

- cmake, ninja, pkg-config

## Build

Set `GRPC_PATH` and `EJFAT_PATH`env variables to gRPC and EJFat paths.

```sh

cmake  --preset  default

cmake  --build  --preset  default

```

## Run

```sh

./build/lclstream-fastcache <config> # default: config/default.json

```

Config keys: `inurl`, `outurl`, `num_workers`, `io_threads`, `hwm`.

## Config

This section describes the meaning of each field in the provided configuration.

#### General config

`inurl`: Example: ```tcp://localhost:5001```

Incoming ZeroMQ URL where the process receives messages to. Typically a tcp:// address with the node's IP where the fastcache is running.

`outurl`: Example:  ```tcp://localhost:5556```

Outgoing ZMQ URL where the receivers can connect to.

`workerurl`: Example:  ```inproc://worker```

Url for the inproc worker threads to use for communication.

`type`:   Application mode type. Controls **runtime pipeline behavior**

- `0`: Simple forward
- `1`: zmq_proxy forward
- `2`: bind inproc forward
- `3`: bind connect forward
- `4`: lock-free queue forward with two threads, this is the **default** mode.
- `5`: lock-free router forward
- `6`: lock-free rep forward
- `7`: lock-free EJFat forward with data from lclstreamer
- `8`: lock-free EJFat forward with simulated data

`helper_threads`: Number of extra worker threads used (only mode 2 and 3).

`zmq_io_threads`: Number of ZMQ bakcground i/o threads in the main context.

`hwm`: High water mark, limits queued messages.

`timeout`: Set in milliseconds. To block forever set to -1. Only counted once the receiver thread started working.

`verbose`: Enables logging of queue size.

`dataMB`: Event size when simulated data is used (for now only mode 8).

#### Metrics config

`metrics`: Enables metrics to be sent via an ipc socket. Available only for type 4. IPC socket address is "ipc:///tmp/fastcache-metrics-receiver(/sender)-12345" (number is the cache id).

`metrics_interval`: Number of messages before metrics are sent to the ipc socket.

`cache_id`: Unique identifier for the ipc metrics url.

#### EJFat config

`ejfat_use_LB`: Use load balancer (LB) with EJFat output.

`mtu`: MTU size, 9000 default.

`sndbufsize`: Send buffer size for the socket (recommended to use wmem_max)

`rateGbps`: Limit the send rate in Gbit/s.

`numSendSockets`: Number of send sockets per thread.

`dataId`: EJFat data source ID number. Use different with multiple senders.

`dataSimulatorThreads`: Number of data simulator worker threads launched.
