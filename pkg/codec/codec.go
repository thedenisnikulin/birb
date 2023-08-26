package codec

type (
	Encode[T any] func(value T) ([]byte, error)
	Decode[T any] func(data []byte) (T, error)
)

type Codec[T any] struct {
	encode Encode[T]
	decode Decode[T]
	tag    string
}

func (c *Codec[T]) Encode(value T) ([]byte, error) {
	return c.encode(value)
}

func (c *Codec[T]) Decode(data []byte) (T, error) {
	return c.decode(data)
}

func (c *Codec[T]) Tag() string {
	return c.tag
}
