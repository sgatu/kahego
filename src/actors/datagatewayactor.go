package actors

import (
	"fmt"
	"strings"
	"sync"

	"sgatu.com/kahego/src/config"
)

type DataGatewayActor struct {
	Actor
	WaitableActor
	StreamsConfig       map[string]config.StreamConfig
	DefaultBucketConfig *config.BucketConfig
	ErrorChannel        chan struct{}
	dataActors          map[string]DataActor
	waitGroupChilds     *sync.WaitGroup
	mDataActors         *sync.Mutex
}

func (dga *DataGatewayActor) OnStart() error {
	dga.dataActors = make(map[string]DataActor)
	dga.waitGroupChilds = &sync.WaitGroup{}
	dga.mDataActors = &sync.Mutex{}
	return nil
}

func (dga *DataGatewayActor) OnStop() error {
	fmt.Println("Waiting for DataActors to close | DataActorGateway")
	for _, actor := range dga.dataActors {
		var act DataActor = actor
		Tell(&act, PoisonPill{})
	}
	dga.waitGroupChilds.Wait()
	return nil
}

func (dga *DataGatewayActor) removeDataActor(streamId string) {
	dga.mDataActors.Lock()
	defer dga.mDataActors.Unlock()
	delete(dga.dataActors, streamId)
}

func (dga *DataGatewayActor) getStreamActor(streamId string) (Actor, error) {
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
		if err != nil {
			fmt.Println("could not initialize DataActor")
			return nil, err
		}
		fmt.Println("initialized new DataActor")
		dga.dataActors[streamId] = dataActor
		return &dataActor, nil
	} else {
		return nil, fmt.Errorf("no configuration found for id %s", streamId)
	}
}

func (dga *DataGatewayActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case PersistMessage:
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
func (dga *DataGatewayActor) GetWorkMethod() DoWorkMethod {
	return dga.DoWork
}
