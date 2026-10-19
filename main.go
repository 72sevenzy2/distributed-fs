package main

import (
	"fmt"

	"github.com/72sevenzy2/file-storage/n2n"
)

func newServer(addr string, nodes ...string) *FileServer {
	tcpOpts := n2n.TCPTransportOpts{
		ListenAddr:  addr,
		Decoder:     &n2n.NOPDecoder{},
		Handshakefn: &n2n.TCPHandshake{},
	}

	tcp := n2n.NewTCPTransport(tcpOpts)

	fileStoreOpts := FileServerOpts{
		StorageRoot:       "some_root",
		PathTransformFunc: TransformPathFunc,
		Transport:         tcp,
		bootStrapNodes:    nodes,
	}

	s := NewFileServer(fileStoreOpts)
	tcpOpts.OnPeer = s.OnPeer

	return s
}

func main() {
	server := newServer(":9000", "")
	server2 := newServer(":8000", ":9000")

	go func() {
		if err := server.Run(); err != nil {
			fmt.Println(err)
			server.Stop()
			return
		}
	}()

	if err := server2.Run(); err != nil {
		fmt.Println(err)
		server2.Stop()
		return
	}

	select {}
}
