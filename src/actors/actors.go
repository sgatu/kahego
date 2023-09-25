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
type InterrumpableActor interface {
	Stop()
}

func InitializeAndStart(actor Actor) {
	fmt.Println("Starting actor", fmt.Sprintf("%T", actor))
	if ia, ok := actor.(InitializableActor); ok {
		err := ia.OnStart()
		if err != nil {
			fmt.Println("Could not start actor due to", err)
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
		close(actor.GetChannel())
	}()
}

func Tell(actor Actor, message interface{}) {
	go func(act Actor, msg interface{}) {
		/*var id string = ""
		if act, ok := act.(IdentifiableActor); ok {
			id = fmt.Sprintf(", has ID %s", act.GetId())
		}
		fmt.Printf("Sending %T to %T%s\n", msg, act, id)*/
		act.GetChannel() <- message
	}(actor, message)
}
func TellIn(actor Actor, message interface{}, wait time.Duration) {
	go func(actor Actor, message interface{}, wait time.Duration) {
		<-time.After(wait)
		actor.GetChannel() <- message
	}(actor, message, wait)
}
