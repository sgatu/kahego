package actors

import (
	"fmt"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type backupDataActor struct {
	Actor
	SupervisedActor
	BaseWaitableActor
	backupStreamConfig config.BackupStreamConfig
	stream             streams.Stream
}

func (bda *backupDataActor) OnStart() error {
	err := bda.initializeStream()
	if err != nil {
		return err
	}
	return nil
}
func (bda *backupDataActor) initializeStream() error {
	strm, err := streams.GetStream(
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
	fmt.Println("Stopping backup data actor | DataActor", bda.GetId())
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
	case *streams.Message:
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
