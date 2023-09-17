package actors

import (
	"fmt"
	"sync"
)

type Actor interface {
	DoWork(message interface{}) (WorkResult, error)
	GetChannel() chan interface{}
}
type SupervisedActor interface {
	GetSupervisor() Actor
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
type WorkResult int

const (
	Continue WorkResult = iota
	Stop
)

type IllChildMessage struct {
	who Actor
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
		if wga, ok := actor.(WaitableActor); ok {
			if wga.GetWaitGroup() != nil {
				defer wga.GetWaitGroup().Done()
			}
		}
		for {
			message := <-actor.GetChannel()
			result, err := actor.DoWork(message)
			if result == Stop || err != nil {
				break
			}
			if err != nil {
				if sa, ok := actor.(SupervisedActor); ok {
					Tell(sa.GetSupervisor(), IllChildMessage{who: actor})
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
	go func() {
		actor.GetChannel() <- message
	}()
}
