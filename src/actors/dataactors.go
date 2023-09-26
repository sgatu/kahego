package actors

import (
	"fmt"
	"strings"
	"sync"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type DataActorGateway struct {
	StreamsConfig   map[string]config.StreamConfig
	ErrorChannel    chan struct{}
	WaitGroup       *sync.WaitGroup
	dataActors      map[string]DataActor
	recvCh          chan interface{}
	waitGroupChilds *sync.WaitGroup
}

func (dga *DataActorGateway) OnStart() error {

	dga.dataActors = make(map[string]DataActor)
	dga.recvCh = make(chan interface{})
	dga.waitGroupChilds = &sync.WaitGroup{}
	return nil
}
func (dga *DataActorGateway) OnStop() error {
	fmt.Println("Waiting for DataActors to close | DataActorGateway")
	for _, actor := range dga.dataActors {
		var act DataActor = actor
		Tell(&act, PoisonPill{})
	}
	dga.waitGroupChilds.Wait()
	return nil
}
func (dga *DataActorGateway) GetChannel() chan interface{} {
	return dga.recvCh
}
func (dga *DataActorGateway) GetWaitGroup() *sync.WaitGroup {
	return dga.WaitGroup
}
func (dga *DataActorGateway) getStreamActor(streamId string) (Actor, error) {
	if actor, ok := dga.dataActors[streamId]; ok {
		return &actor, nil
	}
	if streamConfig, ok := dga.StreamsConfig[streamId]; ok {
		dataActor := DataActor{
			StreamConfig:      streamConfig,
			StreamId:          streamId,
			waitGroup:         dga.waitGroupChilds,
			backupActorConfig: streamConfig.Backup,
			supervisor:        dga,
		}
		err := InitializeAndStart(&dataActor)
		if err == nil {
			dga.dataActors[streamId] = dataActor
			return &dataActor, nil
		}
		return nil, err
	} else {
		return nil, fmt.Errorf("no configuration found for id %s", streamId)
	}

}
func (dga *DataActorGateway) GetWorkMethod() DoWorkMethod {
	return dga.DoWork
}
func (dga *DataActorGateway) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case streams.PersistMessage:
		dataActor, err := dga.getStreamActor(msg.Stream)
		if err == nil {
			Tell(dataActor, msg.Message)
		} else {
			if !strings.Contains(err.Error(), "no configuration found") {
				go func() { dga.ErrorChannel <- struct{}{} }()
				fmt.Println("Could not forward message to DataActor with id", msg.Stream, "| err: ", err)
			}
		}
		return Continue, nil
	case FlushDataMessage:
		dataActor, err := dga.getStreamActor(msg.Stream)
		if err == nil {
			Tell(dataActor, msg)
		}
		return Continue, nil
	case IllChildMessage:
		fmt.Println("DataActor is dead due to:", msg.Error)
		delete(dga.dataActors, msg.Id)
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataGatewayActor\n", msg)
	}
	return Continue, nil
}
func (dga *DataActorGateway) CloseChannel() {
	c := dga.recvCh
	dga.recvCh = nil
	close(c)
}

type BucketActor struct {
	StreamActors     []string
	DataGatewayActor Actor
	Batch            int32
	BatchTimeout     int32
	recvCh           chan interface{}
	processed        int32
}

func (ba *BucketActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case *streams.Message:
		ba.processed++
		flush := false
		if ba.processed >= ba.Batch {
			flush = true
			ba.processed = 0
		}
		for _, dataActorID := range ba.StreamActors {
			Tell(ba.DataGatewayActor, streams.PersistMessage{Message: msg, Stream: dataActorID})
			if flush {
				Tell(ba.DataGatewayActor, FlushDataMessage{Stream: dataActorID})
			}
		}
	default:
		fmt.Printf("Bucket actor received invalid message %T\n", msg)
	}
	return Continue, nil
}

func (ba *BucketActor) OnStart() error {
	ba.recvCh = make(chan interface{})
	return nil
}
func (ba *BucketActor) OnStop() error {
	return nil
}
func (ba *BucketActor) GetWorkMethod() DoWorkMethod {
	return ba.DoWork
}
func (ba *BucketActor) GetChannel() chan interface{} {
	return ba.recvCh
}
func (ba *BucketActor) CloseChannel() {
	c := ba.recvCh
	ba.recvCh = nil
	close(c)
}
