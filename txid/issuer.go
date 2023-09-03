package txid

import (
	"sync"
)

// TODO think of ways to do this without a mutex
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
