package actors

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type AcceptClientActor struct {
	BucketActors    map[string]Actor
	SocketPath      string
	WaitGroup       *sync.WaitGroup
	recvCh          chan interface{}
	socket          *net.UnixListener
	clientIdCounter int
	clients         map[string]ClientHandlerActor
	clientsWG       *sync.WaitGroup
}

func (aca *AcceptClientActor) OnStart() error {
	aca.clients = make(map[string]ClientHandlerActor)
	aca.clientIdCounter = 1
	aca.clientsWG = &sync.WaitGroup{}
	aca.recvCh = make(chan interface{})
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: aca.SocketPath, Net: "unix"})
	if err != nil {
		return err
	}
	aca.socket = socket
	Tell(aca, AcceptNextConnectionMessage{})
	return nil
}

func (aca *AcceptClientActor) GetSupervisor() Actor {
	return nil
}
func (aca *AcceptClientActor) OnStop() error {
	aca.socket.Close()
	aca.clientsWG.Wait()
	if aca.socket != nil {
		aca.socket.Close()
		os.Remove(aca.SocketPath)
	}
	fmt.Println("After cleanup")
	return nil
}

func (aca *AcceptClientActor) GetWaitGroup() *sync.WaitGroup {
	return aca.WaitGroup
}
func (aca *AcceptClientActor) DoWork(message interface{}) (WorkResult, error) {
	switch msg := message.(type) {
	case AcceptNextConnectionMessage:
		//make accept return to continue processing messages
		aca.socket.SetDeadline(time.Now().Add(2 * time.Second))
		conn, err := aca.socket.Accept()
		if err == nil {
			nextClientId := fmt.Sprintf("%d", aca.clientIdCounter)
			handler := ClientHandlerActor{
				client:       conn,
				waitGroup:    aca.clientsWG,
				clientId:     nextClientId,
				bucketActors: aca.BucketActors,
				supervisor:   aca,
			}
			aca.clients[nextClientId] = handler
			aca.clientIdCounter += 1
			InitializeAndStart(&handler)
			Tell(aca, message) //continue loop
			return Continue, nil
		} else {
			if cErr, ok := err.(*net.OpError); ok {
				if errors.Is(cErr.Unwrap(), os.ErrDeadlineExceeded) {
					Tell(aca, message) //continue loop
					return Continue, nil
				}
			}
			return Stop, nil
		}
	case ClientClosedMessage:
		delete(aca.clients, msg.Id)
		return Continue, nil
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Println("Unknown message received by AcceptClientActor")
		return Continue, nil
	}

}
func (aca *AcceptClientActor) GetChannel() chan interface{} {
	return aca.recvCh
}

func (aca *AcceptClientActor) GetWorkMethod() DoWorkMethod {
	return aca.DoWork
}
func (aca *AcceptClientActor) CloseChannel() {
	c := aca.recvCh
	aca.recvCh = nil
	close(c)
}
