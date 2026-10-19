package n2n

import (
	"net"
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
}

func NewPeer(conn net.Conn, outbound bool) *Peer {
	return &Peer{
		Conn:     conn,
		outbound: outbound,
	}
}

// Send() implements the Node interface.
func (n *Peer) Send(b []byte) error {
	_, err := n.Conn.Write(b)
	return err
}
