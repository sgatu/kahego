package actors

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type DataActor struct {
	Actor
	SupervisedActor
	WaitableActor

	BucketId     string
	StreamConfig config.StreamConfig

	stream      streams.Stream
	currentMode DoWorkMethod

	backupActorConfig    *config.BackupStreamConfig
	backupActor          *backupDataActor
	backupActorWaitGroup *sync.WaitGroup
	slice                float32
}

type DataActorError struct {
	Id  string
	Err error
	Who Actor
}

type FlushDataMessage struct{}
type ReviewStream struct{}
type WorkingMode int

const (
	NormalWorkingMode WorkingMode = iota
	BackupWorkingMode
	ErrorWorkinMode
)

func (da *DataActor) OnStart() error {
	err := da.initializeStream()
	if err != nil {
		errB := da.transform(BackupWorkingMode, nil)
		if errB != nil {
			da.transform(ErrorWorkinMode, errB)
		}
	} else {
		da.transform(NormalWorkingMode, nil)
	}
	return nil
}

func (da *DataActor) OnStop() error {
	log.Debugf("Stopping data actor - DataActor Id %s, BucketId %s", da.GetId(), da.BucketId)
	if da.backupActor != nil {
		Tell(da.backupActor, PoisonPill{})
		da.backupActorWaitGroup.Wait()
	}
	if da.stream != nil && !da.stream.HasError() {
		da.stream.Flush()
		da.stream.Close()
	}
	return nil
}

func (da *DataActor) NormalMode(msg interface{}) (WorkResult, error) {
	// check if stream in good shape
	switch msg := msg.(type) {
	case *streams.Message, FlushDataMessage:
		if da.stream == nil || da.stream.HasError() {
			err := da.transform(BackupWorkingMode, nil)
			if err != nil {
				da.transform(ErrorWorkinMode, err)
			}
			return da.currentMode(msg)
		}
	}
	switch msg := msg.(type) {
	case *streams.Message:
		if rand.Float32() < da.slice {
			da.stream.Push(msg)
		}
	case FlushDataMessage:
		log.Tracef("DataActor Id %s, BucketId %s - FlushMessage", da.GetId(), da.BucketId)
		err := da.stream.Flush()
		if err != nil {
			errB := da.transform(BackupWorkingMode, nil)
			if errB != nil {
				da.transform(ErrorWorkinMode, errB)
			}
		}
	default:
		log.Tracef("Unknown message %T for DataActor | NormalMode", msg)
	}
	return Continue, nil
}

func (da *DataActor) BackupMode(msg interface{}) (WorkResult, error) {
	if da.backupActor == nil {
		da.transform(ErrorWorkinMode, fmt.Errorf("no backup actor for stream %s", da.GetId()))
	}
	switch msg := msg.(type) {
	case *streams.Message:
		Tell(da.backupActor, msg)
	case FlushDataMessage:
		Tell(da.backupActor, FlushDataMessage{})
	case ReviewStream:
		if da.initializeStream() == nil {
			da.transform(NormalWorkingMode, nil)
		} else {
			TellIn(da, ReviewStream{}, time.Second*10)
		}
	case IllChildMessage:
		da.transform(ErrorWorkinMode, fmt.Errorf("backup actor dead for stream %s", da.GetId()))
	default:
		log.Tracef("Unknown message %T for DataActor | BackupMode", msg)

	}
	return Continue, nil
}

func (da *DataActor) ErrorMode(msg interface{}) (WorkResult, error) {
	//PoisonPill is managed globably
	return Continue, nil
}

func (da DataActor) GetWorkMethod() DoWorkMethod {
	return da.currentMode
}

func (da *DataActor) transform(mode WorkingMode, err error) error {
	switch mode {
	case NormalWorkingMode:
		log.Infof("Data actor %s | bucket %s - transform normal mode", da.GetId(), da.BucketId)
		da.currentMode = da.NormalMode
	case BackupWorkingMode:
		log.Infof("Data actor %s | bucket %s - transform backup mode", da.GetId(), da.BucketId)
		// move message to backup
		if da.backupActorConfig == nil {
			return fmt.Errorf("could not transform actor, no backup config available")
		}
		if da.backupActor == nil {
			da.backupActorWaitGroup = &sync.WaitGroup{}
			da.backupActor = &backupDataActor{
				Actor: &BaseActor{},
				SupervisedActor: &BaseSupervisedActor{
					supervisor: da,
					id:         da.GetId(),
				},
				BaseWaitableActor:  BaseWaitableActor{WaitGroup: da.backupActorWaitGroup},
				backupStreamConfig: *da.backupActorConfig,
				BucketId:           da.BucketId,
			}
			InitializeAndStart(da.backupActor)
		}
		if da.stream != nil {
			if da.stream.GetQueue().Len() > 0 {
				log.Tracef("DataActor id: %s, bucket: %s - Sending %d messages to backup", da.GetId(), da.BucketId, da.stream.GetQueue().Len())
				len := da.stream.GetQueue().Len()
				for i := 0; i < int(len); i++ {
					if val, err := da.stream.GetQueue().Pop(); err != nil {
						Tell(da.backupActor, val.Value)
					}
				}
				da.stream.GetQueue().Clear()
			}
			da.stream.Close()
			da.stream = nil
		}
		da.currentMode = da.BackupMode
		TellIn(da, ReviewStream{}, time.Second*10)
	case ErrorWorkinMode:
		log.Warnf("Data actor %s | bucket %s - transform error mode", da.GetId(), da.BucketId)
		da.currentMode = da.ErrorMode
		err := fmt.Errorf("could not startup data actor %s", err.Error())
		Tell(da.GetSupervisor(), DataActorError{Id: da.GetId(), Err: err, Who: da})
	}
	return nil
}

func (da *DataActor) initializeStream() error {
	da.slice = da.StreamConfig.Slice
	strm, err := streams.GetStream(da.StreamConfig, da.BucketId)
	if err != nil {
		return err
	}
	da.stream = strm
	return nil
}
