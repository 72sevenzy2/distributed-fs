package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/72sevenzy2/file-storage/n2n"
)

type FileServerOpts struct {
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         n2n.Transport
	bootStrapNodes    []string
}

type FileServer struct {
	FileServerOpts
	store *Storage

	mu       sync.Mutex
	Peers    map[string]*n2n.Peer
	quitChan chan struct{}
}

func NewFileServer(fs FileServerOpts) *FileServer {
	storeOpts := StorageOpts{
		Root:              fs.StorageRoot,
		PathTransformFunc: fs.PathTransformFunc,
	}

	return &FileServer{
		FileServerOpts: fs,
		store:          NewStorage(storeOpts),
		Peers:          make(map[string]*n2n.Peer),
		quitChan:       make(chan struct{}),
	}
}

// Payload is the data which will be sent between peer communication.
type Payload struct {
	Key  string
	Data []byte
}

func (fs *FileServer) StoreData(key string, r io.Reader) error {
	if err := fs.store.Write(key, r); err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, r)
	if err != nil {
		return err
	}

	p := &Payload{
		Key:  key,
		Data: buf.Bytes(),
	}
	fmt.Println(buf.Bytes())

	return fs.broadcast(p)
}

func (fs *FileServer) broadcast(p *Payload) error {
	peers := []io.Writer{}
	for _, peer := range fs.Peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	return gob.NewEncoder(mw).Encode(p)
}

func (fs *FileServer) Stop() {
	close(fs.quitChan)
}

func (s *FileServer) OnPeer(p n2n.Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Peers[p.RemoteAddr().String()] = &p

	log.Printf("connected with remote peer %s", p.RemoteAddr().String())
	return nil
}

func (fs *FileServer) loop() {
	defer func() {
		log.Println("file server stopped running.")
		fs.Transport.Close() // cleanup after file server has closed.
	}()

	for {
		select {
		case msg := <-fs.Transport.Consume():
			fmt.Println(msg)
		case <-fs.quitChan:
			return
		}
	}
}

func (fs *FileServer) bootstrapNetwork() error {
	for _, addr := range fs.bootStrapNodes {
		if addr == "" { // skip empty strings given as peer addresses
			continue
		}

		go func(addr string) {
			if err := fs.Transport.Dial(addr); err != nil {
				fmt.Println("dial err", err)
			}
		}(addr)
	}
	return nil
}

func (fs *FileServer) Run() error {
	if err := fs.Transport.ListenAndAccept(); err != nil {
		return err
	}
	if len(fs.bootStrapNodes) != 0 { // fast path
		fs.bootstrapNetwork()
	}

	fs.loop()

	return nil
}
