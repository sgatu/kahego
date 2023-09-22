package actors

import (
	"fmt"
	"strings"
	"sync"

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
		dataActorChan := make(chan interface{})
		strm, err := stream.GetStream(streamConfig, dataActorChan)
		if err != nil {
			return nil, err
		}
		dataActor := DataActor{
			WaitGroup:        dga.waitGroupChilds,
			Stream:           strm,
			recvCh:           dataActorChan,
			backupActor:      streamConfig.Backup,
			StreamId:         streamId,
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
	recvCh           chan interface{}
	StreamId         string
	Stream           stream.Stream
	WaitGroup        *sync.WaitGroup
	backupActor      *string
	dataGatewayActor Actor
}
type FlushDataMessage struct{ Stream string }

func (da *DataActor) OnStart() error {
	if da.recvCh == nil {
		da.recvCh = make(chan interface{})
	}
	return nil
}
func (da *DataActor) OnStop() error {
	fmt.Println("Stopping data actor | DataActor", da.StreamId)
	da.Stream.Flush()
	da.Stream.Close()
	return nil
}
func (da *DataActor) GetChannel() chan interface{} {
	return da.recvCh
}
func (da *DataActor) GetWaitGroup() *sync.WaitGroup {
	return da.WaitGroup
}
func (da *DataActor) GetId() string {
	return da.StreamId
}
func (da *DataActor) DoWork(msg interface{}) (WorkResult, error) {
	fmt.Printf("DataActor processing %T\n", msg)
	switch msg := msg.(type) {
	case *stream.Message:
		if da.Stream.InBackupMode() && da.backupActor != nil {
			Tell(da.dataGatewayActor, stream.PersistMessage{Stream: *da.backupActor, Message: msg})
		} else {
			da.Stream.Push(msg)
		}
	case FlushDataMessage:
		var err error = nil
		if !da.Stream.InBackupMode() {
			err = da.Stream.Flush()
		} else {
			if da.backupActor != nil {
				Tell(da.dataGatewayActor, FlushDataMessage{Stream: *da.backupActor})
			}
		}
		if err != nil {
			fmt.Println("Here1", err)
			return Stop, err
		}
	case PoisonPill:
		return Stop, nil
	default:
		fmt.Printf("Unknown message %T for DataActor\n", msg)
	}
	return Continue, nil
}
func GetSupervisor(da *DataActor) Actor {
	return da.dataGatewayActor
}

type BucketActor struct {
	recv             chan interface{}
	StreamActors     []string
	DataGatewayActor Actor
	Batch            int32
	BatchTimeout     int32
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
