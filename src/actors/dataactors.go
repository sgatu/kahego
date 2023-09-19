package actors

import (
	"fmt"
	"sync"

	"sgatu.com/kahego/src/stream"
)

type DataActor struct {
	recv      chan interface{}
	Stream    stream.Stream
	WaitGroup *sync.WaitGroup
}
type FlushDataMessage struct{}

func (da *DataActor) OnStart() error {
	da.recv = make(chan interface{})
	return nil
}
func (da *DataActor) OnStop() error {
	da.Stream.Flush()
	da.Stream.Close()
	return nil
}
func (da *DataActor) GetChannel() chan interface{} {
	return da.recv
}
func (da *DataActor) GetWaitGroup() *sync.WaitGroup {
	return da.WaitGroup
}
func (da *DataActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case *stream.Message:
		//fmt.Printf("Data actor received %v\n", msg)
		da.Stream.Push(msg)
	case FlushDataMessage:
		da.Stream.Flush()
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DatActor", msg)
	}
	return Continue, nil
}

type BucketActor struct {
	recv         chan interface{}
	StreamActors []Actor
	Batch        int32
	BatchTimeout int32
	processed    int32
}

func (ba *BucketActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg.(type) {
	case *stream.Message:
		ba.processed++
		flush := false
		if ba.processed >= ba.Batch {
			flush = true
			ba.processed = 0
		}
		for _, dataActor := range ba.StreamActors {
			Tell(dataActor, msg)
			if flush {
				Tell(dataActor, FlushDataMessage{})
			}
		}
	default:
		fmt.Printf("Bucket actor received invalid message %T\n", msg)
	}
	return Continue, nil
}
func (ba *BucketActor) GetChannel() chan interface{} {
	return ba.recv
}
func (ba *BucketActor) OnStart() error {
	ba.recv = make(chan interface{})
	return nil
}
func (ba *BucketActor) OnStop() error {
	return nil
}
