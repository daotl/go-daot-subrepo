# go-log

DAOT Labs' fork of [ipfs/go-log](https://github.com/ipfs/go-log).

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![](https://img.shields.io/badge/project-DAOT%20Labs-red.svg?style=flat-square)](http://github.com/daotl)
[![Go Reference](https://pkg.go.dev/badge/github.com/daotl/go-log/v2.svg)](https://pkg.go.dev/github.com/daotl/go-log/v2)

go-log wraps [zap](https://github.com/uber-go/zap) to provide a logging facade. go-log manages logging
instances and allows for their levels to be controlled individually.

## Additional features of this fork

### Added formats

- `CompactOutput`: a compact format which looks like:
```
D[2021-05-13T17:49:52.413+0800]	example/log.go:52	for Debug
I[2021-05-13T17:49:52.413+0800]	example/log.go:52	for Info
W[2021-05-13T17:49:52.413+0800]	example/log.go:52	for Warn
E[2021-05-13T17:49:52.413+0800]	example/log.go:52	for Error
p[2021-05-13T17:49:52.413+0800]	example/log.go:52	for DPanic
P[2021-05-13T17:49:52.413+0800]	example/log.go:52	for Panic
F[2021-05-13T17:49:52.413+0800]	example/log.go:52	for Fatal
```
- `ColorizedCompactOutput`: same as `CompactOutput` but colorized

### Added config options

- `AutoColor`: automatically switches between formats and their colorized counterparts
  (`ColorizedOutput` <-> `PlaintextOutput`, `ColorizedCompactOutput` <-> `CompactOutput`),
  if the current program is run from a terminal it switches to the colorized version, vice versa
- `AutoStdout`: automatically enables stdout output if the current program is run from a terminal,
  or ((File is not set or not correct) and (URL is not set))
- `Sampling`: configs log sampling
- `Lumberjack`: configs log rolling using [Lumberjack](https://github.com/natefinch/lumberjack)

## Install

```sh
go get github.com/daotl/go-log/v2
```

## Usage

Once the package is imported under the name `logging`, an instance of `EventLogger` can be created like so:

```go
var log = logging.Logger("subsystem name")
```

It can then be used to emit log messages in plain printf-style messages at seven standard levels:

Levels may be set for all loggers:

```go
lvl, err := logging.LevelFromString("error")
if err != nil {
	panic(err)
}
logging.SetAllLoggers(lvl)
```

or individually:

```go
err := logging.SetLogLevel("net:pubsub", "info")
if err != nil {
	panic(err)
}
```

or by regular expression:

```go
err := logging.SetLogLevelRegex("net:.*", "info")
if err != nil {
	panic(err)
}
```

### Environment Variables

This package can be configured through various environment variables.

#### `GOLOG_LOG_LEVEL`

Specifies the log-level, both globally and on a per-subsystem basis.

For example, the following will set the global minimum log level to `error`, but reduce the minimum
log level for `subsystem1` to `info` and reduce the minimum log level for `subsystem2` to debug.

```bash
export GOLOG_LOG_LEVEL="error,subsystem1=info,subsystem2=debug"
```

#### `GOLOG_FILE`

Specifies that logs should be written to the specified file. If this option is _not_ specified, logs are written to standard error.

```bash
export GOLOG_FILE="/path/to/my/file.log"
```

#### `GOLOG_OUTPUT`

Specifies where logging output should be written. Can take one or more of the following values, combined with `+`:

- `stdout` -- write logs to standard out.
- `stderr` -- write logs to standard error.
- `file` -- write logs to the file specified by `GOLOG_FILE`

For example, if you want to log to both a file and standard error:

```bash
export GOLOG_FILE="/path/to/my/file.log"
export GOLOG_OUTPUT="stderr+file"
```

Setting _only_ `GOLOG_FILE` will prevent logs from being written to standard error.

#### `GOLOG_LOG_FMT`

Specifies the log message format. It supports the following values:

- `color` -- human readable, colorized (ANSI) output.
- `nocolor` -- human readable, plain-text output.
- `json` -- structured JSON.
- `compactcolor` -- human readable, compact colorized (ANSI) output.
- `compactnocolor` -- human readable, compact output.

For example, to log structured JSON (for easier parsing):

```bash
export GOLOG_LOG_FMT="json"
```

The logging format defaults to `color` when the output is a terminal, and `nocolor` otherwise.

#### `GOLOG_AUTO_COLOR`

See `AutoColor` in [Added config options](#added-config-options).

#### `GOLOG_LOG_LABELS`

Specifies a set of labels that should be added to all log messages as comma-separated key-value
pairs. For example, the following add `{"app": "example_app", "dc": "sjc-1"}` to every log entry.

```bash
export GOLOG_LOG_LABELS="app=example_app,dc=sjc-1"
```

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/daotl/go-log/issues)!

## License

MIT
