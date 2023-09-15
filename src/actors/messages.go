package actors

import (
	"net"
)

type PoisonPill struct{}
type NewClient struct {
	Conn net.Conn
}
type Message struct {
	bucket string
	data   []byte
	key    []byte
}
