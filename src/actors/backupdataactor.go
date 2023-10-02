package actors

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/streams"
)

type backupDataActor struct {
	Actor
	SupervisedActor
	BaseWaitableActor
	backupStreamConfig config.BackupStreamConfig
	stream             streams.Stream
	BucketId           string
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
			Slice:    1.0,
			Settings: bda.backupStreamConfig.Settings,
		},
		bda.BucketId,
	)
	if err != nil {
		return err
	}
	bda.stream = strm
	return nil
}
func (bda *backupDataActor) OnStop() error {
	log.Debug(fmt.Sprintf("Stopping backup data actor | DataActor %s of bucket %s", bda.GetId(), bda.BucketId))
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
		log.Trace(fmt.Sprintf("Unknown message %T for backupDataActor", msg))
	}
	return Continue, nil
}
