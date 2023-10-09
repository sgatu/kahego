package actors

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testActor struct {
	Actor
	InnerText   string
	count       int64
	channelDone chan struct{}
}

func (ta *testActor) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		switch mCasted := msg.(type) {
		case string:
			ta.InnerText = ta.InnerText + mCasted
			ta.count++
			if ta.count > 2 {
				return Stop, nil
			}
		default:
		}
		return Continue, nil
	}
}
func (ta *testActor) OnStart() error {
	return nil
}
func (ta *testActor) OnStop() error {
	close(ta.channelDone)
	return nil
}

func Test_actorSyncAndStop(t *testing.T) {
	act := &testActor{
		Actor:       &BaseActor{},
		channelDone: make(chan struct{}),
	}
	InitializeAndStart(act)
	Tell(act, "abc")
	<-time.After(100 * time.Microsecond)
	Tell(act, "def")
	<-time.After(100 * time.Microsecond)
	Tell(act, "ghi")
	select {
	case <-act.channelDone:
	case <-time.After(1000 * time.Millisecond):
		t.Fatalf("Actor processing timed out")
		close(act.channelDone)
	}
	if act.InnerText != "abcdefghi" {
		t.Errorf("Actor expected to process messages in order. Expected: abcdefghi, Current: %s", act.InnerText)
	}
}

type childActor struct {
	Actor
	SupervisedActor
	errorMsg string
}

func (ca *childActor) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		return Stop, fmt.Errorf(ca.errorMsg)
	}
}
func (ca *childActor) OnStart() error {
	return nil
}
func (ca *childActor) OnStop() error {
	return nil
}

type parentActor struct {
	Actor
	childId      string
	errorMessage string
	channelDone  chan struct{}
}

func (pa *parentActor) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		switch mCasted := msg.(type) {
		case IllChildMessage:
			pa.childId = mCasted.Id
			pa.errorMessage = mCasted.Error.Error()
			return Stop, nil
		default:
		}
		return Continue, nil
	}
}
func (pa *parentActor) OnStart() error {
	return nil
}
func (pa *parentActor) OnStop() error {
	close(pa.channelDone)
	return nil
}

func Test_supervisedActor(t *testing.T) {
	childId := "id_child"
	errorMsg := "test_error"

	parent := &parentActor{
		Actor:       &BaseActor{},
		channelDone: make(chan struct{}),
	}
	child := &childActor{
		Actor:           &BaseActor{},
		SupervisedActor: &BaseSupervisedActor{supervisor: parent, id: childId},
		errorMsg:        errorMsg,
	}
	InitializeAndStart(parent)
	InitializeAndStart(child)
	Tell(child, struct{}{})
	select {
	case <-parent.channelDone:
	case <-time.After(1000 * time.Millisecond):
		t.Fatalf("Actor processing timed out")
		close(parent.channelDone)
	}
	if parent.childId != childId {
		t.Errorf("Parent expected to recieve IllChild with id %s, received %s", childId, parent.childId)
	}
	if parent.errorMessage != errorMsg {
		t.Errorf("Parent expected to recieve IllChild with error %s, recevied %s", errorMsg, parent.errorMessage)
	}
}

type childActorWait struct {
	Actor
	WaitableActor
}

func (ca *childActorWait) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		switch msg.(type) {
		case PoisonPill:
			return Stop, nil
		}
		return Continue, nil
	}
}
func (ca *childActorWait) OnStart() error {
	return nil
}
func (ca *childActorWait) OnStop() error {
	return nil
}

type parentActorWaiter struct {
	Actor
	childs      []Actor
	channelDone chan struct{}
	waitGroup   *sync.WaitGroup
}

func (pa *parentActorWaiter) GetWorkMethod() DoWorkMethod {
	return func(msg interface{}) (WorkResult, error) {
		switch msg.(type) {
		case PoisonPill:
			return Stop, nil
		}
		return Continue, nil
	}
}
func (pa *parentActorWaiter) OnStart() error {
	pa.channelDone = make(chan struct{})
	return nil
}
func (pa *parentActorWaiter) OnStop() error {
	for i := 0; i < len(pa.childs); i++ {
		Tell(pa.childs[i], PoisonPill{})
	}
	pa.waitGroup.Wait()
	close(pa.channelDone)
	return nil
}

func Test_testWaiting(t *testing.T) {
	childs := 5
	wgChilds := &sync.WaitGroup{}
	childsArr := make([]Actor, childs)
	for i := 0; i < childs; i++ {
		childActor := childActorWait{
			Actor:         &BaseActor{},
			WaitableActor: &BaseWaitableActor{WaitGroup: wgChilds},
		}
		childsArr[i] = &childActor
	}
	parentActor := &parentActorWaiter{
		Actor:     &BaseActor{},
		waitGroup: wgChilds,
		childs:    childsArr,
	}
	InitializeAndStart(parentActor)
	for i := 0; i < childs; i++ {
		InitializeAndStart(childsArr[i])
	}
	Tell(parentActor, PoisonPill{})
	select {
	case <-parentActor.channelDone:
	case <-time.After(1000 * time.Millisecond):
		t.Fatalf("Waiting actor processing timed out")
		close(parentActor.channelDone)
	}
}
