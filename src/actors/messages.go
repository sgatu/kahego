package actors

import (
	"net"
)

type PoisonPill struct{}
type NewClient struct {
	Conn net.Conn
}
type ClientHandleNextMessage struct{}
type AcceptNextConnectionMessage struct{}
type ClientClosedMessage struct {
	Id string
}
