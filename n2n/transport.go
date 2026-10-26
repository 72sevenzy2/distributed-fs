package n2n

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
)

type Transport interface {
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
	Dial(string) error
}

type TCPTransportOpts struct {
	ListenAddr  string
	Decoder     Decoder
	Handshakefn Handshaker

	// OnPeer represents the Peers state upon establishing the connection to the server.
	// Allows for pre-flight validation before the connection is established.
	OnPeer func(Peer) error
}

type TCPTransport struct {
	TCPTransportOpts

	Logger   slog.Logger
	listener net.Listener

	// TCPNodeCh represents a channel in which peers will send and receive data.
	TCPNodeCh chan RPC
}

func NOPOnPeer(v Peer) error { return nil }

func NewTCPTransport(opts TCPTransportOpts) Transport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		Logger:           *slog.Default(),
		TCPNodeCh:        make(chan RPC),
	}
}

// Close() implements the Transport interface.
func (n *TCPTransport) Close() error {
	return n.listener.Close()
}

// Consume() implements Transport interface.
func (n *TCPTransport) Consume() <-chan RPC {
	return n.TCPNodeCh
}

// Dial() implements the Transport interface.
func (n *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	n.handleConn(conn, true)
	return nil
}

func (n *TCPTransport) ListenAndAccept() error {
	ln, err := net.Listen("tcp", n.ListenAddr)
	if err != nil {
		return err
	}

	n.listener = ln
	go n.acceptLoop()
	return nil
}

func (n *TCPTransport) acceptLoop() {
	for {
		var conn net.Conn
		var err error

		defer func() {
			fmt.Println("dropping a connection:", err)
			conn.Close()
		}() // runs when the connection is closed.

		conn, err = n.Handshakefn.HandshakeFn(n.listener)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			n.Logger.Error("ERR", "TCP_HANDSHAKE_ERR", err)
			continue
		}

		go func() {
			if err := n.handleConn(conn, false); err != nil {
				log.Fatalf("err handling connection %s", err)
			}
		}()
	}
}

func (n *TCPTransport) handleConn(conn net.Conn, outbound bool) error {
	var err error
	// check if OnPeer func was provided.
	peer := NewPeer(conn, outbound)

	if n.OnPeer != nil {
		if err = n.OnPeer(*peer); err != nil {
			return nil
		}
	}

	RPC := RPC{}
	for {
		n.Logger.Info("INCOMING_CONNECTION", "addr", conn.RemoteAddr().String())

		if err := n.Decoder.Decode(conn, &RPC); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			conn.Close()
			n.Logger.Error("ERR", "TCP_DECODING_ERR", err)
			continue
		}
		RPC.From = conn.RemoteAddr().String()
			peer.Conn = conn

		fmt.Println("waiting for peer to stream.")
		peer.Wg.Add(1)
		n.TCPNodeCh <- RPC
		peer.Wg.Wait()
		fmt.Println("peer stream has concluded.")
	}
}
