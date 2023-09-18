package actors

import (
	"net"
)

type PoisonPill struct{}
type NewClient struct {
	Conn net.Conn
}
