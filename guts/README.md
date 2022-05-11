# GUTS - Go UTilitieS

[![Go Reference](https://pkg.go.dev/badge/github.com/daotl/guts.svg)](https://pkg.go.dev/github.com/daotl/guts)

### [bytes](./bytes)

- Conversions from/to `[]byte` to/from various types.
- Various bytes operations.
- `HexByte`: `[]byte` alias with `MarshalJSON` and `UnmarshalJSON` methods for hex encoding.

### [error](./error)

Error-related utilities.

### [net](./net/net.go)

#### Connect(protoAddr string) (net.Conn, error)

**Connect** dials the given address and returns a net.Conn. The protoAddr argument should be
prefixed with the protocol, eg. "tcp://127.0.0.1:8080" or "unix:///tmp/test.sock"

#### ProtocolAndAddress(listenAddr string) (string, string)

**ProtocolAndAddress** splits an address into the protocol and address components.
For instance, "tcp://127.0.0.1:8080" will be split into "tcp" and "127.0.0.1:8080".
If the address has no protocol prefix, the default is "tcp".

#### GetFreePort() (int, error)

**GetFreePort** gets a free port from the operating system.

### [os](./os/os.go)

#### TrapSignal(logger logger, cb func())

**TrapSignal** catches SIGTERM and SIGINT, executes the cleanup function, and exits with code 0.

#### Exit(s string)

Exit prints string `s` then `os.Exit(1)`.

#### EnsureDir(dir string, mode os.FileMode) error

**EnsureDir** ensures the given directory exists, creating it if necessary.
Errors if the path already exists as a non-directory.

#### FileExists(filePath string) bool

#### CopyFile(src, dst string) error

**CopyFile** copies a file. It truncates the destination file if it exists.

### [mac](./os/mac.go)

#### GetMACInterface() (iface *net.Interface, err error)

**GetMACInterface** returns the network interface associated with the MAC address of the host machine.

#### GetMACStr() (string, error)

**GetMACStr** returns the MAC address of the host machine.

#### GetMACUint64() (uint64, error)

**GetMACStr** returns the MAC address of the host machine represented in uint64.

### [rand](./rand)

Random number, string, bytes generation.

### [service](./service)

Package [service/goprocesssrv](./service/goprocess/service.go) is a service implementation based on
[goprocess](https://github.com/jbenet/goprocess) that supports declaring and ensuring runtime
dependencies between services.

Package [service/suturesrv](./service/suture/service.go) is a service implementation based on
[Suture](https://github.com/thejerf/suture).

### [sync](./sync)

#### [mutex](./sync/mutex.go)

Use `sync.Mutex` and `sync.RWMutex` from this package instead of `sync` so you can build your
program with `deadlock` flag to detect deadlocks
using [go-deadklock](https://github.com/sasha-s/go-deadlock).

#### [ResultNotifier](./sync/result_notifier.go)

`ResultNotifier` can be used for a goroutine to notify others that a job is done and pass the error
occurred or nil if no error. It's based on `sync.Cond`.

## License

Default license for the code in this library is [MIT](LICENSE), see the source code for exceptions.
