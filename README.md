# fastcache

Multi threaded cache that runs on the data transfer nodes to stream data to external facilities.

## Dependencies

- [vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started-vscode?pivots=shell-bash) — manages C++ deps

- cmake, ninja, pkg-config

## Build

```sh

cmake  --preset  default

cmake  --build  --preset  default

```

## Run

```sh

./build/lclstream-fastcache <config> # default: config/default.json

```

Config keys: `inurl`, `outurl`, `num_workers`, `io_threads`, `hwm`.

## Shutdown sequence

Normal termination is triggered when all expected upstream producers have disconnected. The receiver thread monitors producer connection events: once it has seen at least `expected_producers` connections and the active producer count drops to zero, it signals shutdown. The sender thread completes delivery of any messages already in the queue before exiting, at which point it closes the outbound socket. Downstream consumers waiting on that socket see the close as their own termination signal. A `timeout` value greater than zero provides a fallback: if producers were seen but the message stream goes silent for longer than the configured interval, the same shutdown path is triggered.

## Config

This section describes the meaning of each field in the provided configuration.

`inurl`: Example: ```tcp://134.79.23.43:5001```

Incoming ZeroMQ URL where the process receives messages to. Typically a tcp:// address with the node's IP where the fastcache is running.

`outurl`: Example:  ```tcp://134.79.23.43:5556```

Outgoing ZMQ URL where the receivers can connect to.

`type`:   Application mode type. Controls **runtime pipeline behavior**

- `0`: Simple forward
- `1`: zmq_proxy forward
- `2`: bind inproc forward
- `3`: bind connect forward
- `4`: lock-free queue forward with two threads, this is the **default** mode.
- `5`: lock-free router forward
- `6`: lock-free rep forward
- `7`: connection test for sender

`helper_threads`: Number of extra worker threads used (not used in default mode)

`io_threads`: Number of ZMQ bakcground i/o threads in the main context.
  
`hwm`: High water mark, limits queued messages.

`timeout`: Set in milliseconds. To block forever set to -1. Acts as a fallback: if producers have been seen but no message arrives within this window, the process exits. Not counted until at least one producer has connected.

`expected_producers`: Number of upstream producers that must connect before a disconnection can trigger shutdown. Defaults to 1. This prevents an early or stray disconnect from terminating the cache before the intended producers have joined.

`verbose`: Enables logging of queue size.

`metrics`: Enables metrics to be sent via an ipc socket. Available only for type 4. IPC socket address is "ipc:///tmp/fastcache-metrics-receiver(/sender)-12345" (number is the cache id)

`metrics_interval`: Number of messages before metrics are sent to the ipc socket.

`cache_id`: Unique identifier for the ipc metrics url
