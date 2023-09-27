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

type SupervisedActor interface {
	GetSupervisor() Actor
	GetId() string
}

type SupervisorActor struct {
	supervisor Actor
	id         string
}

func (supervisorActor *SupervisorActor) GetSupervisor() Actor {
	return supervisorActor.supervisor
}
func (supervisorActor *SupervisorActor) GetId() string {
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
	GetChannelSync() *sync.WaitGroup
}
type BaseOrderedMessagesActor struct {
	waitGroup *sync.WaitGroup
}

func (baseOrderedMessageActor *BaseOrderedMessagesActor) GetChannelSync() *sync.WaitGroup {
	if baseOrderedMessageActor.waitGroup == nil {
		baseOrderedMessageActor.waitGroup = &sync.WaitGroup{}
	}
	return baseOrderedMessageActor.waitGroup
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
	go func() {
		if wa, ok := actor.(WaitableActor); ok {
			if wa.GetWaitGroup() != nil {
				defer wa.GetWaitGroup().Done()
			}
		}
		for {
			message := <-actor.GetChannel()
			result, err := actor.GetWorkMethod()(message)
			if result == Stop || err != nil {
				break
			}
			if err != nil {
				if sa, ok := actor.(SupervisedActor); ok {
					Tell(sa.GetSupervisor(), IllChildMessage{Who: actor, Error: err, Id: sa.GetId()})
					break
				}
			}
		}
		if ia, ok := actor.(InitializableActor); ok {
			err := ia.OnStop()
			if err != nil {
				fmt.Println("Could not cleanup actor", fmt.Sprintf("%T", actor), "due to", err)
				return
			}
		}
		actor.CloseChannel()
	}()
	return nil
}
func Tell(actor Actor, message interface{}) {
	fmt.Printf("Before telling %T, %T\n", actor, message)
	if orderedActor, ok := actor.(OrderedMessagesActor); ok {
		orderedActor.GetChannelSync().Wait()
		fmt.Printf("Add 1 to messageSync %T, %T\n", actor, message)
		orderedActor.GetChannelSync().Add(1)
	}
	fmt.Printf("After waiting to tell %T, %T\n", actor, message)
	go func(act Actor, msg interface{}) {
		if orderedActor, ok := act.(OrderedMessagesActor); ok {
			defer func() {
				fmt.Printf("Mark last message for %T, %T as done\n", actor, message)
				orderedActor.GetChannelSync().Done()
			}()
		}

		if actor.GetChannel() != nil {
			defer func() {

				if r := recover(); r != nil {
					fmt.Printf("Recover %T, %T, %+v\n", act, msg, r)
					if orderedActor, ok := act.(OrderedMessagesActor); ok {
						defer func() {
							fmt.Printf("Mark last message for %T, %T as done in recover\n", actor, message)
							orderedActor.GetChannelSync().Done()
						}()
					}
				}

			}()
			fmt.Printf("Telling %T, %T\n", actor, message)
			act.GetChannel() <- message
			fmt.Printf("Told %T, %T\n", actor, message)
		}
	}(actor, message)

}
func TellIn(actor Actor, message interface{}, wait time.Duration) {
	go func(actor Actor, message interface{}, wait time.Duration) {
		<-time.After(wait)
		if actor.GetChannel() != nil {
			actor.GetChannel() <- message
		}
	}(actor, message, wait)
}
