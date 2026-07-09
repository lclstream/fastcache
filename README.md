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
- `5`: place holder for request handler
- `6`: connection test for sender

`helper_threads`: Number of extra worker threads used (not used in default mode)

`io_threads`: Number of ZMQ bakcground i/o threads in the main context.
  
`hwm`: High water mark, limits queued messages.

`timeout`: Set in milliseconds. To block forever set to -1. Only counted once the receiver thread started working.

`verbose`: Enables logging of queue size.
