package n2n

import (
	"net"
	"sync"
)

// Node represents a remote connection.
type Node interface {
	Send([]byte) error
	net.Conn
}

// TCPNode represents the remote nodes connection methodology.
type Peer struct {
	net.Conn
	outbound bool

	Wg *sync.WaitGroup
}

func NewPeer(conn net.Conn, outbound bool) *Peer {
	return &Peer{
		Conn:     conn,
		outbound: outbound,
		Wg:       &sync.WaitGroup{},
	}
}

// Send() implements the Node interface.
func (n *Peer) Send(b []byte) error {
	_, err := n.Conn.Write(b)
	return err
}
