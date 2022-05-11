package error

import (
	"fmt"
)

// ToError converts an `interface{}` to an `error` if it's not one, useful e.g. for recovered values.
func ToError(v any) error {
	if e, ok := v.(error); ok {
		return e
	}
	return fmt.Errorf("%v", v)
}
