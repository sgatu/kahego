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
	Actor
	SupervisedActor
	WaitableActor
	OrderedMessagesActor
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
type FlushDataMessage struct{ Stream string }
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
	fmt.Println("Stopping data actor | DataActor", da.GetId())
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
		err := da.stream.Flush()
		if err != nil {
			errB := da.transform(BackupWorkingMode, nil)
			if errB != nil {
				da.transform(ErrorWorkinMode, errB)
			}
		}
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataActor | NormalMode\n", msg)
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
		Tell(da.backupActor, FlushDataMessage{Stream: ""})
	case PoisonPill:
		return Stop, nil
	case ReviewStream:
		if da.initializeStream() == nil {
			da.transform(NormalWorkingMode, nil)
		} else {
			TellIn(da, ReviewStream{}, time.Second*10)
		}
	case IllChildMessage:
		da.transform(ErrorWorkinMode, fmt.Errorf("backup actor dead for stream %s", da.GetId()))
	default:
		fmt.Printf("Unknown message %T for DataActor | BackupMode\n", msg)

	}
	return Continue, nil
}
func (da *DataActor) ErrorMode(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case PoisonPill:
		fmt.Println("ErrorMode PoisonPill")
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataActor | ErrorMode\n", msg)
	}
	return Continue, nil
}
func (da *DataActor) GetWorkMethod() DoWorkMethod {
	return da.currentMode
}

func (da *DataActor) transform(mode WorkingMode, err error) error {
	switch mode {
	case NormalWorkingMode:
		fmt.Printf("Data actor %s transform normal mode\n", da.GetId())
		da.currentMode = da.NormalMode
	case BackupWorkingMode:
		fmt.Printf("Data actor %s transform backup mode\n", da.GetId())
		// move message to backup
		if da.backupActorConfig == nil {
			return fmt.Errorf("could not transform actor, no backup config available")
		}
		if da.backupActor == nil {
			da.backupActorWaitGroup = &sync.WaitGroup{}
			da.backupActor = &backupDataActor{
				Actor: &BaseActor{},
				SupervisedActor: &SupervisorActor{
					supervisor: da,
					id:         da.GetId(),
				},
				BaseWaitableActor:  BaseWaitableActor{WaitGroup: da.backupActorWaitGroup},
				backupStreamConfig: *da.backupActorConfig,
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
	case ErrorWorkinMode:
		fmt.Printf("Data actor %s transform error mode\n", da.GetId())
		da.currentMode = da.ErrorMode
		err := fmt.Errorf("could not startup data actor %s", err.Error())
		Tell(da.GetSupervisor(), DataActorError{Id: da.GetId(), Err: err, Who: da})
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
