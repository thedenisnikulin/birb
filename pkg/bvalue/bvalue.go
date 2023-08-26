// provides type from binary value representation
package bvalue

import "strconv"

// binary value
type Value []byte

func FromInt[I ~int](v I) Value {
	return Value([]byte(strconv.FormatInt(int64(v), 10)))
}

func FromString[S ~string](v S) Value {
	return []byte(v)
}
