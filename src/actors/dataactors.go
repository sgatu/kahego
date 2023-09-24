package actors

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/stream"
)

type DataActorGateway struct {
	StreamsConfig   map[string]config.StreamConfig
	ErrorChannel    chan struct{}
	WaitGroup       *sync.WaitGroup
	dataActors      map[string]DataActor
	recvCh          chan interface{}
	waitGroupChilds *sync.WaitGroup
}

func (dga *DataActorGateway) OnStart() error {
	dga.dataActors = make(map[string]DataActor)
	dga.recvCh = make(chan interface{})
	dga.waitGroupChilds = &sync.WaitGroup{}
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
func (dga *DataActorGateway) GetChannel() chan interface{} {
	return dga.recvCh
}
func (dga *DataActorGateway) GetWaitGroup() *sync.WaitGroup {
	return dga.WaitGroup
}
func (dga *DataActorGateway) getStreamActor(streamId string) (Actor, error) {
	if actor, ok := dga.dataActors[streamId]; ok {
		return &actor, nil
	}
	if streamConfig, ok := dga.StreamsConfig[streamId]; ok {
		dataActor := DataActor{
			StreamConfig:     streamConfig,
			StreamId:         streamId,
			waitGroup:        dga.waitGroupChilds,
			backupActor:      streamConfig.Backup,
			dataGatewayActor: dga,
		}
		InitializeAndStart(&dataActor)
		dga.dataActors[streamId] = dataActor
		dga.waitGroupChilds.Add(1)
		return &dataActor, nil
	} else {
		return nil, fmt.Errorf("no configuration found for id %s", streamId)
	}

}
func (dga *DataActorGateway) GetWorkMethod() DoWorkMethod {
	return dga.DoWork
}
func (dga *DataActorGateway) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case stream.PersistMessage:
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
		delete(dga.dataActors, msg.id)
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataGatewayActor\n", msg)
	}
	return Continue, nil
}

type DataActor struct {
	StreamId     string
	StreamConfig config.StreamConfig

	waitGroup        *sync.WaitGroup
	stream           stream.Stream
	recvCh           chan interface{}
	backupActor      *string
	dataGatewayActor Actor
	currentMode      DoWorkMethod
}
type FlushDataMessage struct{ Stream string }
type ReviewStream struct{}

func (da *DataActor) OnStart() error {
	da.recvCh = make(chan interface{})
	da.transform(false)
	err := da.initializeStream()
	if err != nil {
		da.transform(true)
	}
	return nil
}
func (da *DataActor) transform(inBackup bool) {
	if !inBackup {
		fmt.Printf("Data actor %s transform normal mode\n", da.StreamId)
		da.currentMode = da.NormalMode
	} else {
		fmt.Printf("Data actor %s transform backup mode\n", da.StreamId)
		// move message to backup
		if da.backupActor != nil && da.stream.GetQueue().Len() > 0 {
			fmt.Printf("Sending %d messages to backup\n", da.stream.GetQueue().Len())
			len := da.stream.GetQueue().Len()
			for i := 0; i < int(len); i++ {
				if val, err := da.stream.GetQueue().Pop(); err != nil {
					Tell(da.dataGatewayActor, stream.PersistMessage{Stream: *da.backupActor, Message: val.Value})
				}
			}
			da.stream.GetQueue().Clear()
		}
		da.stream = nil
		da.currentMode = da.BackupMode
		TellIn(da, ReviewStream{}, time.Second*60)
	}
}
func (da *DataActor) OnStop() error {
	fmt.Println("Stopping data actor | DataActor", da.StreamId)
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
func (da *DataActor) initializeStream() error {
	strm, err := stream.GetStream(da.StreamConfig)
	if err != nil {
		return err
	}
	da.stream = strm
	return nil
}
func (da *DataActor) NormalMode(msg interface{}) (WorkResult, error) {
	//fmt.Printf("DataActor processing %T\n", msg)
	if da.stream.HasError() {
		da.transform(true)
		return da.currentMode(msg)
	}
	switch msg := msg.(type) {
	case *stream.Message:
		da.stream.Push(msg)
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
	case *stream.Message:
		Tell(da.dataGatewayActor, stream.PersistMessage{Stream: *da.backupActor, Message: msg})
	case FlushDataMessage:
		Tell(da.dataGatewayActor, FlushDataMessage{Stream: *da.backupActor})
	case PoisonPill:
		return Stop, nil
	case ReviewStream:
		fmt.Println("Review Stream...")
		if da.initializeStream() == nil {
			da.transform(false)
		} else {
			TellIn(da, ReviewStream{}, time.Second*60)
		}
	default:
		fmt.Printf("Unknown message %T for DataActor | BackupMode %t\n", msg, true)

	}
	return Continue, nil
}
func GetSupervisor(da *DataActor) Actor {
	return da.dataGatewayActor
}
func (da *DataActor) GetWorkMethod() DoWorkMethod {
	return da.currentMode
}

type BucketActor struct {
	StreamActors     []string
	DataGatewayActor Actor
	Batch            int32
	BatchTimeout     int32
	recv             chan interface{}
	processed        int32
}

func (ba *BucketActor) DoWork(msg interface{}) (WorkResult, error) {
	switch msg := msg.(type) {
	case *stream.Message:
		ba.processed++
		flush := false
		if ba.processed >= ba.Batch {
			flush = true
			ba.processed = 0
		}
		for _, dataActorID := range ba.StreamActors {
			Tell(ba.DataGatewayActor, stream.PersistMessage{Message: msg, Stream: dataActorID})
			if flush {
				Tell(ba.DataGatewayActor, FlushDataMessage{Stream: dataActorID})
			}
		}
	default:
		fmt.Printf("Bucket actor received invalid message %T\n", msg)
	}
	return Continue, nil
}
func (ba *BucketActor) GetChannel() chan interface{} {
	return ba.recv
}
func (ba *BucketActor) OnStart() error {
	ba.recv = make(chan interface{})
	return nil
}
func (ba *BucketActor) OnStop() error {
	return nil
}
func (ba *BucketActor) GetWorkMethod() DoWorkMethod {
	return ba.DoWork
}
