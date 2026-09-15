# fastcache

Multi threaded cache that runs on the data transfer nodes to stream data to external facilities.

## Dependencies

- [vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started-vscode?pivots=shell-bash) — manages C++ deps

- cmake, ninja, pkg-config

## Build

<<<<<<< HEAD
### Build with vcpkg
=======
Set `GRPC_PATH` and `EJFAT_PATH`env variables to gRPC and EJFat paths.
>>>>>>> e76c8a4 (Added basic EJFat worker)

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

`metrics_interval`: (optional, default: `10000`) Number of messages before metrics are sent to the ipc socket.

`cache_id`: (optional, default: `1`) Unique identifier for the ipc metrics url

#### EJFat config

`ejfat_use_LB`: Use load balancer (LB) with EJFat output.

`mtu`: MTU size, 9000 default.

`sndbufsize`: Send buffer size for the socket (recommended to use wmem_max)

`rateGbps`: Limit the send rate in Gbit/s.

`numSendSockets`: Number of send sockets per thread.

`dataId`: EJFat data source ID number. Use different with multiple senders.

`dataSimulatorThreads`: Number of data simulator worker threads launched.

`queueSize`: EJFat event queue size (smaller is better if the events are large). Example: 40
