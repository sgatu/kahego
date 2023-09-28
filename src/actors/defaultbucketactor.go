package actors

import (
	"fmt"

	"sgatu.com/kahego/src/streams"
)

type DefaultBucketActor struct {
	Actor
	StreamActors     []string
	DataGatewayActor Actor
	Batch            int32
	BatchTimeout     int32
	processed        int32
}
type PersistDefaultMessage struct {
	Message *streams.Message
	Stream  string
	Key     string
}

func (ba *DefaultBucketActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case *streams.Message:
		ba.processed++
		flush := false
		if ba.processed >= ba.Batch {
			flush = true
			ba.processed = 0
		}
		for _, dataActorID := range ba.StreamActors {
			Tell(ba.DataGatewayActor, PersistDefaultMessage{Message: msg, Stream: dataActorID})
			if flush {
				Tell(ba.DataGatewayActor, FlushDataMessage{Stream: dataActorID})
			}
		}
	default:
		fmt.Printf("Bucket actor received invalid message %T\n", msg)
	}
	return Continue, nil
}

// override GetWorkMethod
func (ba *DefaultBucketActor) GetWorkMethod() DoWorkMethod {
	return ba.DoWork
}
