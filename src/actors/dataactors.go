package actors

import (
	"fmt"

	"sgatu.com/kahego/src/stream"
)

type DataActor struct {
	recv   chan interface{}
	Stream stream.Stream
}
type FlushDataMessage struct{}

func (da *DataActor) OnStart() error {
	da.recv = make(chan interface{})
	return nil
}
func (da *DataActor) OnStop() error {
	return nil
}
func (da *DataActor) GetChannel() chan interface{} {
	return da.recv
}
func (da *DataActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case stream.Message:
		da.Stream.Push(msg)
	case FlushDataMessage:
		da.Stream.Flush()
	default:
		fmt.Printf("Unknown message %T for DatActor", msg)
	}
	return Continue, nil
}

type BucketActor struct {
	Bucket    stream.Bucket
	processed int32
}

func (ba *BucketActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case stream.Message:
		ba.processed++
		for _, stream := range ba.Bucket.Streams {
			stream.Push(msg)
			if ba.processed >= ba.Bucket.Batch {
				stream.Flush()
			}
		}
		if ba.processed >= ba.Bucket.Batch {
			ba.processed = 0
		}
	}
	return Continue, nil
}
