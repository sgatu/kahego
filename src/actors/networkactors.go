package actors

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type ClientHandleNextMessage struct{}
type AcceptNextConnectionMessage struct{}
type ClientHandlerActor struct {
	recv           chan interface{}
	client         net.Conn
	waitGroup      *sync.WaitGroup
	clientFinished chan int
	clientId       int
}

func (cha *ClientHandlerActor) GetChannel() chan interface{} {
	return cha.recv
}
func (cha *ClientHandlerActor) DoWork(message interface{}) (WorkResult, error) {
	switch message.(type) {
	case ClientHandleNextMessage:
		msg := make([]byte, 4)
		len, err := cha.client.Read(msg)
		if err != nil {
			cha.client.Close()
			return Stop, nil
		}
		if len < 4 {
			fmt.Println("Invalid data length")
			return Continue, nil
		}
		msgLen := binary.LittleEndian.Uint32(msg)
		msg = make([]byte, 0, msgLen)
		countRead := uint32(0)
		for countRead < msgLen {
			bufferSize := min(1024, msgLen-countRead)
			buffer := make([]byte, bufferSize)
			len, err = cha.client.Read(buffer)
			if err != nil {
				cha.client.Close()
				return Stop, nil
			}
			countRead += uint32(len)
			msg = append(msg, buffer[:len]...)
		}
		go func(msg []byte) {
			message, err := GetMessage(msg)
			if err == nil {
				fmt.Printf("%+v\n", message)
			} else {
				fmt.Println(err)
			}
		}(msg)
		Tell(cha, message)
	default:
		fmt.Println("Unknown message received by ClientHandlerActor")
	}
	return Continue, nil
}
func (cha *ClientHandlerActor) OnStart() error {
	cha.recv = make(chan interface{})
	Tell(cha, ClientHandleNextMessage{})
	return nil
}
func (cha *ClientHandlerActor) OnStop() error {
	fmt.Println("Shutting down client", cha.clientId)
	<-time.After(time.Second * 2)
	fmt.Println("Client shutted down", cha.clientId)
	cha.clientFinished <- cha.clientId
	cha.waitGroup.Done()
	return nil
}
func (cha *ClientHandlerActor) Stop() {
	cha.client.Close()
}

type AcceptClientActor struct {
	recv               chan interface{}
	SocketPath         string
	socket             *net.UnixListener
	isChannelClosed    bool
	WaitGroup          *sync.WaitGroup
	clientIdCounter    int
	clients            map[int]ClientHandlerActor
	clientsWG          *sync.WaitGroup
	clientFinishedChan chan int
}

func (aca *AcceptClientActor) OnStart() error {
	aca.clients = make(map[int]ClientHandlerActor)
	aca.clientFinishedChan = make(chan int, 500)
	aca.clientIdCounter = 1
	aca.clientsWG = &sync.WaitGroup{}
	aca.isChannelClosed = false
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: aca.SocketPath, Net: "unix"})
	if err != nil {
		return err
	}
	aca.socket = socket
	aca.recv = make(chan interface{})
	Tell(aca, AcceptNextConnectionMessage{})
	go func() {
		for {
			i, more := <-aca.clientFinishedChan
			fmt.Println("Cleaning disconnected client", i)
			delete(aca.clients, i)
			if !more {
				fmt.Println("Closing finished channel")
				break
			}
		}
	}()
	return nil
}
func (aca *AcceptClientActor) GetChannel() chan interface{} {
	return aca.recv
}
func (aca *AcceptClientActor) GetSupervisor() Actor {
	return nil
}
func (aca *AcceptClientActor) OnStop() error {
	fmt.Println("Waiting for clients to get closed")
	for _, handler := range aca.clients {
		handler.Stop()
	}
	aca.clientsWG.Wait()
	close(aca.clientFinishedChan)
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
func (aca *AcceptClientActor) Stop() {
	aca.socket.Close()
}
func (aca *AcceptClientActor) DoWork(message interface{}) (WorkResult, error) {
	switch message.(type) {
	case AcceptNextConnectionMessage:
		conn, err := aca.socket.Accept()
		if err == nil {
			handler := ClientHandlerActor{
				recv:           make(chan interface{}),
				client:         conn,
				waitGroup:      aca.clientsWG,
				clientId:       aca.clientIdCounter,
				clientFinished: aca.clientFinishedChan,
			}
			aca.clientsWG.Add(1)
			aca.clients[aca.clientIdCounter] = handler
			aca.clientIdCounter += 1
			InitializeAndStart(&handler)
			Tell(aca, message) //continue loop
			return Continue, nil
		} else {
			return Stop, nil
		}
	default:
		fmt.Println("Unknown message received by AcceptClientActor")
		return Continue, nil
	}

}
