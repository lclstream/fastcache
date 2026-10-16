# fastcache

Multi threaded cache that runs on the data transfer nodes to stream data to external facilities.

## Dependencies

- [vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started-vscode?pivots=shell-bash) — manages C++ deps

- cmake, ninja, pkg-config

## Build

### Build with vcpkg

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

`inurl`: Example: ```tcp://134.79.23.43:5001```

Incoming ZeroMQ URL where the process receives messages to. Typically a tcp:// address with the node's IP where the fastcache is running.

`outurl`: (optional, default: `""`) Example:  ```tcp://134.79.23.43:5556```

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

`helper_threads`: (optional, default: `0`) Number of extra worker threads used (not used in default mode)

`io_threads`: (optional, default: `16`) Number of ZMQ bakcground i/o threads in the main context.

`hwm`: (optional, default: `10`) High water mark, limits queued messages.

`timeout`: (optional, default: `-1`) Set in milliseconds. To block forever set to -1. Only counted once the receiver thread started working.

`verbose`: (optional, default: `false`) Enables logging of queue size.

`metrics`: (optional, default: `false`) Enables metrics to be sent via an ipc socket. Available only for type 4. IPC socket address is "ipc:///tmp/fastcache-metrics-receiver(/sender)-12345" (number is the cache id)

`metrics_interval`: (optional, default: `10000`) Number of messages before metrics are sent to the ipc socket.

`cache_id`: (optional, default: `1`) Unique identifier for the ipc metrics url

`simdata_size_mb`: (optional, default: `32`) Size of simulated data in MB (mode `TestOut` only)

`datasim_threads`: (optional, default: `1`) Number of workers that simulate data (mode `TestOut` only)
