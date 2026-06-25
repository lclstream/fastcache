# fastcache

Multi threaded cache that runs on the data transfer nodes to stream data to external facilities.

## Dependencies

- [vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started-vscode?pivots=shell-bash) — manages C++ deps
- cmake, ninja, pkg-config

## Build

```sh
cmake --preset default
cmake --build --preset default
```

## Run

```sh
./build/lclstream-fastcache <config>    # default: config/default.json
```

Config keys: `inurl`, `outurl`, `num_workers`, `io_threads`, `hwm`.
