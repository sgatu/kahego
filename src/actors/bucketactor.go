package actors

import (
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type CheckFlushNeeded struct{}
type BucketActor struct {
	Actor
	WaitableActor
	SupervisedActor
	BucketConfig     config.BucketConfig
	BucketId         string
	waitGroupStreams *sync.WaitGroup
	dataActors       map[string]*DataActor
	processed        int32
	lastFlush        int64
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
			log.Warn(fmt.Sprintf("Could not initialize DataActor with id %s, bucketId %s", dataActor.GetId(), dataActor.BucketId))
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
				log.Warn(fmt.Sprintf("Could not forward message to DataActor - BucketId %s, StreamId %s, err %s", ba.BucketConfig.BucketId, id, err))
				return Stop, err
			}
			Tell(actor, msg)
			if flush {
				ba.lastFlush = time.Now().UnixMilli()
				Tell(actor, FlushDataMessage{})
			}
		}
	case CheckFlushNeeded:
		if time.Now().UnixMilli()-ba.lastFlush > int64(ba.BucketConfig.BatchTimeout) {
			for _, actor := range ba.dataActors {
				Tell(actor, FlushDataMessage{})
			}
			ba.lastFlush = time.Now().UnixMilli()
		}
		TellIn(ba, CheckFlushNeeded{}, time.Duration(ba.BucketConfig.BatchTimeout)*time.Millisecond)
	case DataActorError:
		ba.removeDataActor(msg.Id)
		Tell(msg.Who, PoisonPill{})
	case IllChildMessage:
		log.Warn(fmt.Sprintf("DataActor is dead due to: %s", msg.Error))
		ba.removeDataActor(msg.Id)
	case PoisonPill:
		return Stop, nil
	default:
		log.Trace(fmt.Sprintf("Bucket actor received invalid message %T", msg))
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
	log.Debug(fmt.Sprintf("Initialized bucket \"%s\" with config id \"%s\"", ba.BucketId, ba.BucketConfig.BucketId))
	ba.waitGroupStreams = &sync.WaitGroup{}
	ba.processed = 0
	ba.dataActors = make(map[string]*DataActor)
	ba.lastFlush = time.Now().UnixMilli()
	TellIn(ba, CheckFlushNeeded{}, time.Duration(ba.BucketConfig.BatchTimeout)*time.Millisecond)
	return nil
}

// override GetWorkMethod
func (ba *BucketActor) GetWorkMethod() DoWorkMethod {
	return ba.DoWork
}
