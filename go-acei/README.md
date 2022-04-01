# go-acei 

Go implementation of [Application Consensus Engine Interface (ACEI)](https://github.com/daotl/acei).

## Installation & Usage

To get up and running quickly, see the [getting started guide](../docs/app-dev/getting-started.md) along with the [acei-cli documentation](../docs/app-dev/acei-cli.md) which will go through the examples found in the [examples](./example/) directory.

## Specification

A detailed description of the ABCI methods and message types is contained in:

- [The main spec](https://github.com/daotl/acei/blob/master/spec/acei/acei.md)
- [A protobuf file](https://github.com/daotl/acei/blob/master/proto/daotl/acei/types.proto)
- [A Go interface](./types/application.go)

## Protocol Buffers

To compile the protobuf file and generate Go code from [ACEI Protocol Buffers definitions](https://github.com/daotl/acei/tree/master/proto),
run in [acei/proto](https://github.com/daotl/acei) directory:
```sh
protoc --gogofaster_out=. --go-grpc_out=. \
  -I=${GOPATH}/pkg/mod/google.golang.org/protobuf@v1.27.1/types/known/emptypb \
  -I=${GOPATH}/pkg/mod/github.com/gogo/protobuf@v1.3.2 -I=. \
  ./daotl/acei/*.proto

protoc --gogofaster_out=. --go-grpc_out=. \
  -I=${GOPATH}/pkg/mod/google.golang.org/protobuf@v1.27.1/types/known/emptypb \
  -I=${GOPATH}/pkg/mod/github.com/gogo/protobuf@v1.3.2 -I=. \
  ./daotl/acei/*.proto \
  ./daotl/acei/consensus/tendermint/*.proto
```

Or on Windows:
```bat
protoc --gogofaster_out=. --go-grpc_out=. ^
  -I=%GOPATH%/pkg/mod/google.golang.org/protobuf@v1.27.1/types/known/emptypb ^
  -I=%GOPATH%/pkg/mod/github.com/gogo/protobuf@v1.3.2 -I=. ^
  ./daotl/acei/*.proto
  
protoc --gogofaster_out=. --go-grpc_out=. ^
  -I=%GOPATH%/pkg/mod/google.golang.org/protobuf@v1.27.1/types/known/emptypb ^
  -I=%GOPATH%/pkg/mod/github.com/gogo/protobuf@v1.3.2 -I=. ^
  ./daotl/acei/consensus/tendermint/*.proto
```

See `protoc --help` and [the Protocol Buffers site](https://developers.google.com/protocol-buffers)
for details on compiling for other languages. Note we also include a [GRPC](https://www.grpc.io/docs)
service definition.

## License

Apache 2.0

Copyright for portions of this fork are held by Tendermint as part of the original
[Tendermint Core](https://github.com/tendermint/tendermint) project. All other
copyright for this fork are held by DAOT Labs. All rights reserved.
