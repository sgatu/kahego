package actors

import (
	"encoding/binary"
	"fmt"
	"net"

	"sgatu.com/kahego/src/streams"
)

type ClientHandlerActor struct {
	Actor
	WaitableActor
	SupervisedActor
	client       net.Conn
	bucketActors map[string]Actor
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
			message, err := streams.GetMessage(msg)
			if err == nil {
				if actor, ok := cha.bucketActors[message.Bucket]; ok {
					Tell(actor, message)
				}
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
	Tell(cha, ClientHandleNextMessage{})
	return nil
}
func (cha *ClientHandlerActor) OnStop() error {
	Tell(cha.GetSupervisor(), ClientClosedMessage{Id: cha.GetId()})
	return nil
}
func (cha *ClientHandlerActor) Stop() {
	cha.client.Close()
}
func (cha *ClientHandlerActor) GetWorkMethod() DoWorkMethod {
	return cha.DoWork
}
