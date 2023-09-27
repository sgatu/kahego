package actors

import (
	"fmt"
	"strings"
	"sync"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type DataActorGateway struct {
	Actor
	WaitableActor
	StreamsConfig   map[string]config.StreamConfig
	ErrorChannel    chan struct{}
	dataActors      map[string]DataActor
	waitGroupChilds *sync.WaitGroup
	mDataActors     *sync.Mutex
}

func (dga *DataActorGateway) OnStart() error {
	dga.dataActors = make(map[string]DataActor)
	dga.waitGroupChilds = &sync.WaitGroup{}
	dga.mDataActors = &sync.Mutex{}
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

func (dga *DataActorGateway) removeDataActor(streamId string) {
	dga.mDataActors.Lock()
	defer dga.mDataActors.Unlock()
	delete(dga.dataActors, streamId)
}

func (dga *DataActorGateway) getStreamActor(streamId string) (Actor, error) {
	dga.mDataActors.Lock()
	defer dga.mDataActors.Unlock()
	if actor, ok := dga.dataActors[streamId]; ok {
		return &actor, nil
	}
	if streamConfig, ok := dga.StreamsConfig[streamId]; ok {
		dataActor := DataActor{
			StreamConfig: streamConfig,
			Actor:        &BaseActor{},
			SupervisedActor: &BaseSupervisedActor{
				supervisor: dga,
				id:         streamId,
			},
			WaitableActor: &BaseWaitableActor{
				WaitGroup: dga.waitGroupChilds,
			},
			OrderedMessagesActor: &BaseOrderedMessagesActor{},
			backupActorConfig:    streamConfig.Backup,
		}
		err := InitializeAndStart(&dataActor)
		if err == nil {
			fmt.Println("initialized new DataActor")
			dga.dataActors[streamId] = dataActor
			return &dataActor, nil
		}
		fmt.Println("could not initialize DataActor")
		return nil, err
	} else {
		return nil, fmt.Errorf("no configuration found for id %s", streamId)
	}

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
		dga.removeDataActor(msg.Id)
	case DataActorError:
		fmt.Println("Deleting dataActor with id", msg.Id)
		dga.removeDataActor(msg.Id)
		fmt.Printf("Sending to %+v a poisonPill\n", msg.Who)
		Tell(msg.Who, PoisonPill{})
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataGatewayActor\n", msg)
	}
	return Continue, nil
}

// override GetWorkMethod
func (dga *DataActorGateway) GetWorkMethod() DoWorkMethod {
	return dga.DoWork
}

type BucketActor struct {
	Actor
	StreamActors     []string
	DataGatewayActor Actor
	Batch            int32
	BatchTimeout     int32
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

// override GetWorkMethod
func (ba *BucketActor) GetWorkMethod() DoWorkMethod {
	return ba.DoWork
}
