package pb

//go:generate go get -u github.com/gogo/protobuf/protoc-gen-gogofaster
//go:generate protoc -I. --gogofaster_out=paths=source_relative:. pubsubcollect.proto
