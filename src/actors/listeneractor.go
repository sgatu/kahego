package actors

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type AcceptNextConnectionMessage struct{}

type AcceptClientActor struct {
	Actor
	WaitableActor
	BucketMangerActor Actor
	ListenerErrorChan chan struct{}
	SocketPath        string
	socket            *net.UnixListener
	clientIdCounter   int
	clients           map[string]ClientHandlerActor
	clientsWG         *sync.WaitGroup
}

func (aca *AcceptClientActor) OnStart() error {
	aca.clients = make(map[string]ClientHandlerActor)
	aca.clientIdCounter = 1
	aca.clientsWG = &sync.WaitGroup{}
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: aca.SocketPath, Net: "unix"})
	if err != nil {
		return err
	}
	aca.socket = socket
	Tell(aca, AcceptNextConnectionMessage{})
	return nil
}
func (aca *AcceptClientActor) OnStop() error {
	for _, actor := range aca.clients {
		Tell(&actor, PoisonPill{})
	}
	aca.socket.Close()
	aca.clientsWG.Wait()
	if aca.socket != nil {
		aca.socket.Close()
		os.Remove(aca.SocketPath)
	}

	return nil
}

func (aca *AcceptClientActor) DoWork(message interface{}) (WorkResult, error) {
	switch msg := message.(type) {
	case AcceptNextConnectionMessage:
		err := aca.socket.SetDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			close(aca.ListenerErrorChan)
			return Stop, nil
		}
		conn, err := aca.socket.Accept()

		if err == nil {
			nextClientId := fmt.Sprintf("%d", aca.clientIdCounter)
			handler := ClientHandlerActor{
				Actor: &BaseActor{},
				WaitableActor: &BaseWaitableActor{
					WaitGroup: aca.clientsWG,
				},
				SupervisedActor: &BaseSupervisedActor{
					supervisor: aca,
					id:         nextClientId,
				},
				client:            conn,
				bucketMangerActor: aca.BucketMangerActor,
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
			close(aca.ListenerErrorChan)
			return Stop, nil
		}
	case ClientClosedMessage:
		delete(aca.clients, msg.Id)
		return Continue, nil
	default:
		fmt.Println("Unknown message received by AcceptClientActor")
		return Continue, nil
	}

}

// overriding GetWorkMethod
func (aca *AcceptClientActor) GetWorkMethod() DoWorkMethod {
	return aca.DoWork
}
