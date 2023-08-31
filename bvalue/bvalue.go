// Provides types for binary value representation
// TODO deprecate? why is it even used lol
package bvalue

import "strconv"

// binary value
type Value []byte

func (v Value) String() string {
	return string(v)
}

func FromInt[I ~int](v I) Value {
	return Value([]byte(strconv.FormatInt(int64(v), 10)))
}

func FromString[S ~string](v S) Value {
	return []byte(v)
}
