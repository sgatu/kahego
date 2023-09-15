package actors

import "fmt"

type Actor interface {
	OnStart() error
	OnStop() error
	DoWork(message interface{}) (WorkResult, error)
	GetSupervisor() Actor
	GetChannel() chan interface{}
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
	err := actor.OnStart()
	if err != nil {
		fmt.Println("Could not start actor due to", err)
		return
	}
	go func() {
		for {
			message := <-actor.GetChannel()
			stopActor := false
			switch message.(type) {
			case PoisonPill:
				stopActor = true
			default:
				result, err := actor.DoWork(message)
				if result == Stop || err != nil {
					stopActor = true
				}
				if err != nil && actor.GetSupervisor() != nil {
					Tell(actor.GetSupervisor(), IllChildMessage{who: actor})
				}
			}
			if stopActor {
				break
			}

		}
		actor.OnStop()
		close(actor.GetChannel())
	}()
}

func Tell(actor Actor, message interface{}) {

	go func() {
		actor.GetChannel() <- message
	}()
}
