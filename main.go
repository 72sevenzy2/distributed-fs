package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/72sevenzy2/file-storage/n2n"
)

func newServer(addr string, nodes ...string) *FileServer {
	tcpOpts := n2n.TCPTransportOpts{
		ListenAddr:  addr,
		Decoder:     &n2n.NOPDecoder{},
		Handshakefn: &n2n.TCPHandshake{},
	}

	var s *FileServer
	tcpOpts.OnPeer = func(p n2n.Peer) error {
		return s.OnPeer(p)
	}
	tcp := n2n.NewTCPTransport(tcpOpts)

	fileStoreOpts := FileServerOpts{
		StorageRoot:       "some_root",
		PathTransformFunc: TransformPathFunc,
		Transport:         tcp,
		bootStrapNodes:    nodes,
	}

	s = NewFileServer(fileStoreOpts)

	return s
}

func main() {
	server := newServer(":9000", "")
	server2 := newServer(":8000", ":9000")

	go func() {
		if err := server.Run(); err != nil {
			fmt.Println(err)
		}
	}()
	time.Sleep(2 * time.Second)

	go func() {
		if err := server2.Run(); err != nil {
			fmt.Println(err)
		}
	}()
	time.Sleep(2 * time.Second)

	data := bytes.NewReader([]byte("some big data"))
	if err := server2.StoreData("privatedata", data); err != nil {
		fmt.Println(err)
	}

	select {}
}
