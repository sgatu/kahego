package actors

import (
	"fmt"
	"sync"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/stream"
)

type backupDataActor struct {
	streamId           string
	backupStreamConfig config.BackupStreamConfig
	waitGroup          *sync.WaitGroup
	stream             stream.Stream
	recvCh             chan interface{}
	supervisor         Actor
}

func (bda *backupDataActor) OnStart() error {
	bda.recvCh = make(chan interface{})
	err := bda.initializeStream()
	if err != nil {
		return err
	}
	return nil
}
func (bda *backupDataActor) initializeStream() error {
	strm, err := stream.GetStream(
		config.StreamConfig{
			Type:     bda.backupStreamConfig.Type,
			Key:      bda.backupStreamConfig.Key,
			Slice:    1.0,
			Settings: bda.backupStreamConfig.Settings,
		},
	)
	if err != nil {
		return err
	}
	bda.stream = strm
	return nil
}
func (bda *backupDataActor) OnStop() error {
	fmt.Println("Stopping backup data actor | DataActor", bda.streamId)
	if bda.stream != nil {
		bda.stream.Flush()
		bda.stream.Close()
	}
	return nil
}

func (bda *backupDataActor) GetWorkMethod() DoWorkMethod {
	return bda.DoWork
}
func (bda *backupDataActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case *stream.Message:
		if bda.stream.HasError() {
			return Stop, bda.stream.GetError()
		}
		bda.stream.Push(msg)
	case FlushDataMessage:
		if bda.stream.HasError() {
			return Stop, bda.stream.GetError()
		}
		err := bda.stream.Flush()
		if err != nil {
			return Stop, err
		}
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for backupDataActor\n", msg)
	}
	return Continue, nil
}
func (bda *backupDataActor) GetChannel() chan interface{} {
	return bda.recvCh
}
func (bda *backupDataActor) GetSupervisor() Actor {
	return bda.supervisor
}
func (bda *backupDataActor) GetId() string {
	return bda.streamId
}
func (bda *backupDataActor) GetWaitGroup() *sync.WaitGroup {
	return bda.waitGroup
}
