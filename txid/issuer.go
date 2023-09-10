package txid

import (
	"sync"
)

type Issuer interface {
	Issue() ID
}

// Zero value is a valid instance
type MxIssuer struct {
	latest ID
	mx     sync.Mutex
}

func (c *MxIssuer) Issue() ID {
	c.mx.Lock()
	defer c.mx.Unlock()

	id := c.latest.Inc()
	c.latest = id
	return id
}

type AtomicIssuer struct {
	latest *AtomicID
}

func (c AtomicIssuer) Issue() ID {
	return c.latest.Inc().ToID()
}

func NewAtomicIssuer() AtomicIssuer {
	return AtomicIssuer{new(AtomicID)}
}
