package actors

import (
	"fmt"
	"sync"
	"time"
)

type WorkResult int

const (
	Continue WorkResult = iota
	Stop
)

type IllChildMessage struct {
	Who   Actor
	Id    string
	Error error
}
type DoWorkMethod func(msg interface{}) (WorkResult, error)
type Actor interface {
	Init()
	GetWorkMethod() DoWorkMethod
	GetChannel() chan interface{}
	CloseChannel()
}
type BaseActor struct {
	recvCh chan interface{}
}

func (baseActor *BaseActor) Init() {
	baseActor.recvCh = make(chan interface{})
}

func (baseActor *BaseActor) GetChannel() chan interface{} {
	return baseActor.recvCh
}
func (baseActor *BaseActor) CloseChannel() {
	c := baseActor.recvCh
	baseActor.recvCh = nil
	close(c)
}

func (baseActor *BaseActor) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		fmt.Printf("BaseActor message processing, received a %T, you should override this method.\n", msg)
		return Continue, nil
	}
}

type QueuedChannelActor struct {
	recvCh       chan interface{}
	ChannelQueue int
}

func (queuedChannelActor *QueuedChannelActor) Init() {
	queuedChannelActor.recvCh = make(chan interface{}, queuedChannelActor.ChannelQueue)
}

func (queuedChannelActor *QueuedChannelActor) GetChannel() chan interface{} {
	return queuedChannelActor.recvCh
}
func (queuedChannelActor *QueuedChannelActor) CloseChannel() {
	c := queuedChannelActor.recvCh
	queuedChannelActor.recvCh = nil
	close(c)
}
func (queuedChannelActor *QueuedChannelActor) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		fmt.Printf("QueuedChannelActor message processing, received a %T, you should override this method.\n", msg)
		return Continue, nil
	}
}

type SupervisedActor interface {
	GetSupervisor() Actor
	GetId() string
}

type BaseSupervisedActor struct {
	supervisor Actor
	id         string
}

func (supervisorActor *BaseSupervisedActor) GetSupervisor() Actor {
	return supervisorActor.supervisor
}
func (supervisorActor *BaseSupervisedActor) GetId() string {
	return supervisorActor.id
}

type InitializableActor interface {
	OnStart() error
	OnStop() error
}

type WaitableActor interface {
	GetWaitGroup() *sync.WaitGroup
}
type BaseWaitableActor struct {
	WaitGroup *sync.WaitGroup
}

func (baseWaitableActor *BaseWaitableActor) GetWaitGroup() *sync.WaitGroup {
	return baseWaitableActor.WaitGroup
}

type OrderedMessagesActor interface {
	GetChannelSync() chan struct{}
}
type BaseOrderedMessagesActor struct {
	doneChan chan struct{}
}

func (baseOrderedMessagesActorV2 *BaseOrderedMessagesActor) GetChannelSync() chan struct{} {
	if baseOrderedMessagesActorV2.doneChan == nil {
		baseOrderedMessagesActorV2.doneChan = make(chan struct{})
	}
	return baseOrderedMessagesActorV2.doneChan
}

func InitializeAndStart(actor Actor) error {
	fmt.Println("Starting actor", fmt.Sprintf("%T", actor))
	actor.Init()
	if ia, ok := actor.(InitializableActor); ok {
		err := ia.OnStart()
		if err != nil {
			fmt.Println("Could not start actor due to", err)
			return err
		}
	}
	if wa, ok := actor.(WaitableActor); ok {
		if wa.GetWaitGroup() != nil {
			wa.GetWaitGroup().Add(1)
		}
	}
	if oma, ok := actor.(OrderedMessagesActor); ok {
		go func() {
			// first message processing
			oma.GetChannelSync() <- struct{}{}
		}()
	}
	go func() {
		if wa, ok := actor.(WaitableActor); ok {
			if wa.GetWaitGroup() != nil {
				defer wa.GetWaitGroup().Done()
			}
		}

		for {
			message := <-actor.GetChannel()
			result, err := actor.GetWorkMethod()(message)
			if result == Stop {
				if err != nil {
					if sa, ok := actor.(SupervisedActor); ok {
						Tell(sa.GetSupervisor(), IllChildMessage{Who: actor, Error: err, Id: sa.GetId()})
					}
				}
				break
			}
			if oma, ok := actor.(OrderedMessagesActor); ok {
				oma.GetChannelSync() <- struct{}{}
			}
		}
		if ia, ok := actor.(InitializableActor); ok {
			err := ia.OnStop()
			if err != nil {
				fmt.Println("Could not cleanup actor", fmt.Sprintf("%T", actor), "due to", err)
				return
			}
		}
		if oma, ok := actor.(OrderedMessagesActor); ok {
			close(oma.GetChannelSync())
		}
		actor.CloseChannel()
	}()
	return nil
}
func Tell(actor Actor, message interface{}) {
	/*if orderedActor, ok := actor.(OrderedMessagesActorV2); ok {
		orderedActor.GetChannelSync().Wait()
		orderedActor.GetChannelSync().Add(1)
	}*/
	go func(act Actor, msg interface{}) {
		if actor.GetChannel() != nil {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Recover %T, %T, %+v\n", act, msg, r)
				}

			}()
			/*	if orderedActor, ok := act.(OrderedMessagesActor); ok {
				orderedActor.GetChannelSync().Done()
			}*/
			if orderedActor, ok := act.(OrderedMessagesActor); ok {
				if _, more := <-orderedActor.GetChannelSync(); more {
					act.GetChannel() <- message
				}
			} else {
				act.GetChannel() <- message
			}

		}
	}(actor, message)

}
func TellIn(actor Actor, message interface{}, wait time.Duration) {
	go func(actor Actor, message interface{}, wait time.Duration) {
		<-time.After(wait)
		Tell(actor, message)
	}(actor, message, wait)
}
