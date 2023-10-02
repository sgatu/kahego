package actors

import (
	"fmt"
	"strings"
	"sync"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type BucketActor struct {
	Actor
	WaitableActor
	SupervisedActor
	BucketConfig     config.BucketConfig
	BucketId         string
	waitGroupStreams *sync.WaitGroup
	dataActors       map[string]*DataActor
	processed        int32
}

func (ba *BucketActor) getStreamActor(streamId string) (*DataActor, error) {
	if actor, ok := ba.dataActors[streamId]; ok {
		return actor, nil
	}
	if streamConfig, ok := ba.BucketConfig.StreamConfigs[streamId]; ok {
		dataActor := DataActor{
			StreamConfig: streamConfig,
			Actor:        &BaseActor{},
			BucketId:     ba.BucketId,
			SupervisedActor: &BaseSupervisedActor{
				supervisor: ba,
				id:         streamId,
			},
			WaitableActor: &BaseWaitableActor{
				WaitGroup: ba.waitGroupStreams,
			},
			OrderedMessagesActor: &BaseOrderedMessagesActor{},
			backupActorConfig:    streamConfig.Backup,
		}
		err := InitializeAndStart(&dataActor)
		if err != nil {
			fmt.Println("could not initialize DataActor")
			return nil, err
		}
		ba.dataActors[streamId] = &dataActor
		return &dataActor, nil
	} else {
		return nil, fmt.Errorf("no configuration found for id %s", streamId)
	}
}
func (ba *BucketActor) removeDataActor(streamId string) {
	delete(ba.dataActors, streamId)
}

func (ba *BucketActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case *streams.Message:
		ba.processed++
		flush := false
		if ba.processed >= ba.BucketConfig.Batch {
			flush = true
			ba.processed = 0
		}
		for id := range ba.BucketConfig.StreamConfigs {
			actor, err := ba.getStreamActor(id)
			if err != nil && !strings.Contains(err.Error(), "no configuration found") {
				fmt.Println("Could not forward message to DataActor | BucketId", ba.BucketConfig.BucketId, ", StreamId", id, "| err: ", err)
				return Stop, err
			}
			Tell(actor, msg)
			if flush {
				Tell(actor, FlushDataMessage{})
			}
		}
	case DataActorError:
		ba.removeDataActor(msg.Id)
		Tell(msg.Who, PoisonPill{})
	case IllChildMessage:
		fmt.Println("DataActor is dead due to:", msg.Error)
		ba.removeDataActor(msg.Id)
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Bucket actor received invalid message %T\n", msg)
	}
	return Continue, nil
}
func (ba *BucketActor) OnStop() error {
	for _, actor := range ba.dataActors {
		Tell(actor, PoisonPill{})
	}
	ba.waitGroupStreams.Wait()

	return nil
}
func (ba *BucketActor) OnStart() error {
	fmt.Println("Initialized bucket \"" + ba.BucketId + "\" with config id \"" + ba.BucketConfig.BucketId + "\"")
	ba.waitGroupStreams = &sync.WaitGroup{}
	ba.processed = 0
	ba.dataActors = make(map[string]*DataActor)
	return nil
}

// override GetWorkMethod
func (ba *BucketActor) GetWorkMethod() DoWorkMethod {
	return ba.DoWork
}
