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
	GetWorkMethod() DoWorkMethod
	GetChannel() chan interface{}
	CloseChannel()
}
type SupervisedActor interface {
	GetSupervisor() Actor
	GetId() string
}
type InitializableActor interface {
	OnStart() error
	OnStop() error
}
type WaitableActor interface {
	GetWaitGroup() *sync.WaitGroup
}
type OrderedMessagesActor interface {
	GetChannelSync() *sync.WaitGroup
}

func InitializeAndStart(actor Actor) error {
	fmt.Println("Starting actor", fmt.Sprintf("%T", actor))
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
	if orderedActor, ok := actor.(OrderedMessagesActor); ok {
		orderedActor.GetChannelSync().Wait()
		orderedActor.GetChannelSync().Add(1)
	}

	go func(act Actor, msg interface{}) {
		if orderedActor, ok := act.(OrderedMessagesActor); ok {
			defer orderedActor.GetChannelSync().Done()
		}
		if actor.GetChannel() != nil {
			defer func() {

				if r := recover(); r != nil {
					fmt.Printf("Recover %T, %T, %+v\n", act, msg, r)
				}

			}()
			act.GetChannel() <- message
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
