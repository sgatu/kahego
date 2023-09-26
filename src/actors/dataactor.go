package actors

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type DataActor struct {
	StreamId     string
	StreamConfig config.StreamConfig

	waitGroup   *sync.WaitGroup
	stream      streams.Stream
	recvCh      chan interface{}
	currentMode DoWorkMethod

	supervisor           Actor
	backupActorConfig    *config.BackupStreamConfig
	backupActor          *backupDataActor
	backupActorWaitGroup *sync.WaitGroup
	slice                float32
}
type FlushDataMessage struct{ Stream string }
type ReviewStream struct{}

func (da *DataActor) OnStart() error {
	da.recvCh = make(chan interface{})
	err := da.initializeStream()
	err = da.transform(err != nil)
	return err
}
func (da *DataActor) OnStop() error {
	fmt.Println("Stopping data actor | DataActor", da.StreamId)
	if da.backupActor != nil {
		Tell(da.backupActor, PoisonPill{})
		da.backupActorWaitGroup.Wait()
	}
	if da.stream != nil {
		da.stream.Flush()
		da.stream.Close()
	}
	return nil
}
func (da *DataActor) GetChannel() chan interface{} {
	return da.recvCh
}
func (da *DataActor) GetWaitGroup() *sync.WaitGroup {
	return da.waitGroup
}
func (da *DataActor) GetId() string {
	return da.StreamId
}
func (da *DataActor) NormalMode(msg interface{}) (WorkResult, error) {
	// check if stream in good shape
	switch msg := msg.(type) {
	case *streams.Message, FlushDataMessage:
		if da.stream == nil || da.stream.HasError() {
			err := da.transform(true)
			if err != nil {
				return Stop, err
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
		err := da.stream.Flush()
		if err != nil {
			return Stop, err
		}
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataActor | BackupMode %t\n", msg, false)
	}
	return Continue, nil
}
func (da *DataActor) BackupMode(msg interface{}) (WorkResult, error) {
	if da.backupActor == nil {
		return Stop, da.stream.GetError()
	}
	switch msg := msg.(type) {
	case *streams.Message:
		Tell(da.backupActor, msg)
	case FlushDataMessage:
		Tell(da.backupActor, FlushDataMessage{Stream: ""})
	case PoisonPill:
		return Stop, nil
	case ReviewStream:
		if da.initializeStream() == nil {
			da.transform(false)
		} else {
			TellIn(da, ReviewStream{}, time.Second*10)
		}
	case IllChildMessage:
		return Stop, fmt.Errorf("backup actor dead too, err: %s", msg.Error.Error())
	default:
		fmt.Printf("Unknown message %T for DataActor | BackupMode %t\n", msg, true)

	}
	return Continue, nil
}
func (da *DataActor) GetSupervisor() Actor {
	return da.supervisor
}
func (da *DataActor) GetWorkMethod() DoWorkMethod {
	return da.currentMode
}

func (da *DataActor) transform(inBackup bool) error {
	if !inBackup {
		fmt.Printf("Data actor %s transform normal mode\n", da.StreamId)
		da.currentMode = da.NormalMode
	} else {
		fmt.Printf("Data actor %s transform backup mode\n", da.StreamId)
		// move message to backup
		if da.backupActorConfig == nil {
			return fmt.Errorf("could not transform actor, no backup config available")
		}
		if da.backupActor == nil {
			da.backupActorWaitGroup = &sync.WaitGroup{}
			da.backupActor = &backupDataActor{
				waitGroup:          da.backupActorWaitGroup,
				streamId:           da.StreamId,
				backupStreamConfig: *da.backupActorConfig,
				supervisor:         da,
			}
			InitializeAndStart(da.backupActor)
		}
		if da.stream != nil {
			if da.stream.GetQueue().Len() > 0 {
				fmt.Printf("Sending %d messages to backup\n", da.stream.GetQueue().Len())
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
	}
	return nil
}
func (da *DataActor) initializeStream() error {
	da.slice = da.StreamConfig.Slice
	strm, err := streams.GetStream(da.StreamConfig)
	if err != nil {
		return err
	}
	da.stream = strm
	return nil
}
func (da *DataActor) IsAvailable() bool {
	return true
}
func (da *DataActor) CloseChannel() {
	c := da.recvCh
	da.recvCh = nil
	close(c)
}
