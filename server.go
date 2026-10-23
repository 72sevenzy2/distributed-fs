package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

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

// MessageStoreFile represents the File metadata in which is sent over the peer network for replication.
type MessageStoreFile struct {
	Key  string
	Data []byte
}

type Message struct {
	Payload any
}

func (fs *FileServer) StoreData(key string, r io.Reader) error {
	var buf bytes.Buffer
	msg := Message{
		Payload: []byte("test data"),
	}
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		return err
	}

	var wire bytes.Buffer
	if err := gob.NewEncoder(&wire).Encode(buf.Bytes()); err != nil {
		return err
	}

	for _, peer := range fs.Peers {
		if err := peer.Send(wire.Bytes()); err != nil {
			return err
		}
	}

	time.Sleep(time.Second * 3)

	payload := []byte("this is a large file")
	for _, peer := range fs.Peers {
		if err := peer.Send(payload); err != nil {
			return err
		}
	}

	// buf := new(bytes.Buffer)
	// tee := io.teereader(r, buf)
	//
	// if err := fs.store.Write(key, tee); err != nil {
	// 	return err
	// }
	// p := Message{
	// 	Payload: &MessageStoreFile{
	// 		Key:  key,
	// 		Data: buf.Bytes(),
	// 	},
	// }
	//
	// return fs.broadcast(&p)
	return nil
}

func (fs *FileServer) broadcast(p *Message) error {
	var payload bytes.Buffer
	// encoding to bytes initially.
	if err := gob.NewEncoder(&payload).Encode(p); err != nil {
		return err
	}

	peers := []io.Writer{}
	for _, peer := range fs.Peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	// encode payload.Bytes() as the receiving gob.Decoder expects a byte slice.
	return gob.NewEncoder(mw).Encode(payload.Bytes())
}

func (fs *FileServer) Stop() {
	close(fs.quitChan)
}

func (s *FileServer) OnPeer(p n2n.Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Peers[p.RemoteAddr().String()] = &p
	fmt.Println(s.Peers)

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
			var p Message
			if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&p); err != nil {
				log.Printf("GOB decoding error %s", err)
			}
			fmt.Printf("received %s\n", string(p.Payload.([]byte)))

			peer, ok := fs.Peers[msg.From]
			if !ok {
				panic("peer not found in peer map")
			}
			fmt.Println(peer)

			b := make([]byte, 10000) // blocks
			if _, err := peer.Read(b); err != nil {
				panic(err)
			}

			fmt.Printf("received big data %s\n", string(b))

			// if err := fs.handlePayload(&p); err != nil {
			// 	log.Println(err)
			// }
			//
		case <-fs.quitChan:
			return
		}
	}
}

//	func (fs *FileServer) handlePayload(p *Message) error {
//		switch v := p.Payload.(type) {
//		case MessageStoreFile:
//			fmt.Printf("received data %+v\n", string(v.Data))
//		}
//
//		return nil
//	}
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

// preloading
func init() {
	gob.Register(MessageStoreFile{})
}
