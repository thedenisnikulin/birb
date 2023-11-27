package byteutil

func NewSeqWriter[T any]() SeqWriter[T] {
	return SeqWriter[T]{make([]T, 0), 0}
}

type SeqWriter[T any] struct {
	buf []T
	off int
}

func (w *SeqWriter[T]) Write(p []T) (n int, err error) {
	copied := copy(w.buf[w.off:], p)
	w.off += copied
	return copied, nil
}

func (w *SeqWriter[T]) Slice() []T {
	return w.buf
}

func (w *SeqWriter[T]) Offset() int {
	return w.off
}

func (w *SeqWriter[T]) Len() int {
	return len(w.buf)
}

func Uint16ToByteSlice(n uint16) []byte {
	arr := [2]byte{byte(n), byte(n << 8)}
	return arr[:]
}
