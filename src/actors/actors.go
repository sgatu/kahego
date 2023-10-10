package actors

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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
type PoisonPill struct{}
type DoWorkMethod func(msg interface{}) (WorkResult, error)
type Actor interface {
	Init()
	GetWorkMethod() DoWorkMethod
	GetChannel() chan interface{}
	CloseChannel()
	GetChannelSync() chan struct{}
}
type BaseActor struct {
	recvCh   chan interface{}
	doneChan chan struct{}
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
func (baseActor *BaseActor) GetChannelSync() chan struct{} {
	if baseActor.doneChan == nil {
		baseActor.doneChan = make(chan struct{}, 1)
	}
	return baseActor.doneChan
}
func (baseActor *BaseActor) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		log.Warnf("BaseActor message processing, received a %T, you should override this method.", msg)
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

func InitializeAndStart(actor Actor) error {
	log.Debugf("Starting actor %T", actor)
	actor.Init()
	// first message processing
	actor.GetChannelSync() <- struct{}{}
	if ia, ok := actor.(InitializableActor); ok {
		err := ia.OnStart()
		if err != nil {
			log.Warnf("Could not start actor due to %s", err)
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
			var result WorkResult
			var err error
			if _, ok := message.(PoisonPill); ok {
				result, err = Stop, nil
			} else {
				result, err = actor.GetWorkMethod()(message)
			}
			if result == Stop {
				if err != nil {
					if sa, ok := actor.(SupervisedActor); ok {
						Tell(sa.GetSupervisor(), IllChildMessage{Who: actor, Error: err, Id: sa.GetId()})
					}
				}
				break
			}
			actor.GetChannelSync() <- struct{}{}
		}
		if ia, ok := actor.(InitializableActor); ok {
			err := ia.OnStop()
			if err != nil {
				log.Warnf("Could not cleanup actor %s due to %s", fmt.Sprintf("%T", actor), err)
				return
			}
		}

		close(actor.GetChannelSync())
		actor.CloseChannel()
	}()
	return nil
}

func Tell(actor Actor, message interface{}) {
	go func(act Actor, msg interface{}) {
		if actor.GetChannel() != nil {
			if _, more := <-act.GetChannelSync(); more {
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
