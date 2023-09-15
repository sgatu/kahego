package actors

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type ClientHandleNextMessage struct{}
type AcceptNextConnectionMessage struct{}
type ClientHandlerActor struct {
	recv   chan interface{}
	client net.Conn
}

func (cha *ClientHandlerActor) GetChannel() chan interface{} {
	return cha.recv
}
func (cha *ClientHandlerActor) DoWork(message interface{}) (WorkResult, error) {
	switch message.(type) {
	case ClientHandleNextMessage:
		msg := make([]byte, 500)
		len, err := cha.client.Read(msg)
		if err != nil {
			fmt.Println("Last message read returned error, closing actor...")
			cha.client.Close()
			return Stop, nil
		}
		fmt.Print("Received msg of length", fmt.Sprintf("%d", len), ": ", string(msg))
		Tell(cha, message)
	default:
		fmt.Println("Unknown message received by ClientHandlerActor")
	}
	return Continue, nil
}
func (cha *ClientHandlerActor) GetSupervisor() Actor {
	return nil
}
func (cha *ClientHandlerActor) OnStart() error {
	cha.recv = make(chan interface{})
	Tell(cha, ClientHandleNextMessage{})
	return nil
}
func (cha *ClientHandlerActor) OnStop() error {
	return nil
}

type AcceptClientActor struct {
	recv             chan interface{}
	StopAndCleanedUp chan struct{}
	SocketPath       string
	socket           *net.UnixListener
	isChannelClosed  bool
}

func (aca *AcceptClientActor) OnStart() error {
	aca.isChannelClosed = false
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: aca.SocketPath, Net: "unix"})
	if err != nil {
		return err
	}
	aca.socket = socket
	aca.socket.SetDeadline(time.Now().Add(500 * time.Millisecond))
	aca.recv = make(chan interface{})
	Tell(aca, AcceptNextConnectionMessage{})
	return nil
}
func (aca *AcceptClientActor) GetChannel() chan interface{} {
	return aca.recv
}
func (aca *AcceptClientActor) GetSupervisor() Actor {
	return nil
}
func (aca *AcceptClientActor) OnStop() error {

	if aca.socket != nil {
		aca.socket.Close()
		os.Remove(aca.SocketPath)
	}
	fmt.Println("After cleanup")
	aca.StopAndCleanedUp <- struct{}{}
	return nil
}

func (aca *AcceptClientActor) DoWork(message interface{}) (WorkResult, error) {
	switch message.(type) {
	case AcceptNextConnectionMessage:
		conn, err := aca.socket.Accept()
		aca.socket.SetDeadline(time.Now().Add(500 * time.Millisecond))
		if err == nil {
			handler := ClientHandlerActor{
				recv:   make(chan interface{}),
				client: conn,
			}
			InitializeAndStart(&handler)
			Tell(aca, message)
			return Continue, nil
		} else {
			if strings.HasSuffix(err.Error(), "i/o timeout") {
				Tell(aca, message)
				return Continue, nil
			}
			return Stop, nil
		}
	default:
		fmt.Println("Unknown message received by AcceptClientActor")
		return Continue, nil
	}

}
