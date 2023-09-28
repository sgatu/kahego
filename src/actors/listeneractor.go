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
	Actor
	WaitableActor
	OrderedMessagesActor
	BucketActors    map[string]Actor
	SocketPath      string
	socket          *net.UnixListener
	clientIdCounter int
	clients         map[string]ClientHandlerActor
	clientsWG       *sync.WaitGroup
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
	fmt.Println("Before cleanup")
	aca.socket.Close()
	aca.clientsWG.Wait()
	if aca.socket != nil {
		aca.socket.Close()
		os.Remove(aca.SocketPath)
	}
	fmt.Println("After cleanup")
	return nil
}

func (aca *AcceptClientActor) DoWork(message interface{}) (WorkResult, error) {
	fmt.Printf("AcceptedClientActor received %T\n", message)
	switch msg := message.(type) {
	case AcceptNextConnectionMessage:
		aca.socket.SetDeadline(time.Now().Add(1 * time.Second))
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
				client:       conn,
				bucketActors: aca.BucketActors,
			}
			aca.clients[nextClientId] = handler
			aca.clientIdCounter += 1
			InitializeAndStart(&handler)
			Tell(aca, message) //continue loop
			return Continue, nil
		} else {
			//fmt.Printf("AcceptedClientActor got error err, %+v\n", err)
			if cErr, ok := err.(*net.OpError); ok {
				if errors.Is(cErr.Unwrap(), os.ErrDeadlineExceeded) {
					Tell(aca, message) //continue loop
					//fmt.Printf("AcceptedClientActor just continue, deadline error\n")
					return Continue, nil
				}
			}
			//fmt.Printf("AcceptedClientActor STOOOP nonDeadLine\n")
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

// overriding GetWorkMethod
func (aca *AcceptClientActor) GetWorkMethod() DoWorkMethod {
	return aca.DoWork
}
