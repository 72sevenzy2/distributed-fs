package n2n

import (
	"encoding/gob"
	"fmt"
	"io"
)

type Decoder interface {
	Decode(io.Reader, *RPC) error
}

type GOBDecoder struct{}

func (g *GOBDecoder) Decode(r io.Reader, v *RPC) error {
	dec := gob.NewDecoder(r)
	err := dec.Decode(&v.Payload)
	if err != nil {
		return err
	}
	return nil
}

type NOPDecoder struct{}

func (n *NOPDecoder) Decode(r io.Reader, v *RPC) error {
	buf := make([]byte, 1024)

	n1, err := r.Read(buf)
	if err != nil {
		return err
	}

	v.Payload = buf[:n1]
	fmt.Println(string(v.Payload))
	return nil
}
