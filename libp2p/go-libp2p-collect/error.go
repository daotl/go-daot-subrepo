package collect

import "errors"

var (
	ErrTopicJoined    = errors.New("ErrTopicJoined")
	ErrTopicNotJoined = errors.New("ErrTopicNotJoined")
	ErrNilTopicWires  = errors.New("ErrNilTopicWires")
)
