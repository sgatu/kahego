package datastructures

import (
	"testing"
)

func Test_queue(t *testing.T) {
	q := NewQueue[int]()
	if q.Len() != 0 {
		t.Fatal("queue length should be 0")
	}
	if q.first != nil {
		t.Fatal("queue first element should be nil")
	}

	_, err := q.Pop()
	if err == nil {
		t.Fatal("empty queue should return error when Pop")
	}
	q.Push(1)
	if q.first == nil || q.first.Value != 1 {
		t.Fatal("first node value is not the same as pushed to queue")
	}
	if q.Len() != 1 {
		t.Fatal("queue len should be 1")
	}
	q.Push(2)
	if q.Len() != 2 {
		t.Fatal("queue len should be 2")
	}
	if q.first.next == nil || q.first.next.Value != 2 {
		t.Fatal("queue linking was not succesful")
	}
	if q.last.next != nil {
		t.Fatal("last queue element should not have a linked node")
	}
	q.Clear()

	if q.Len() != 0 {
		t.Fatalf("queue len should be 0 after clear. Expected 0, Current %d", q.Len())
	}
	if q.first != nil || q.last != nil {
		t.Fatal("queue first node or last node should be nil after clear")
	}
	q.Push(1)
	if q.first == nil || q.first.Value != 1 {
		t.Fatal("After Clear: first node value is not the same as pushed to queue")
	}
	if q.Len() != 1 {
		t.Fatal("After Clear: queue len should be 1")
	}
	q.Clear()

	if q.Len() != 0 {
		t.Fatalf("singleElement: queue len should be 0 after clear. Expected 0, Current %d", q.Len())
	}
	if q.first != nil || q.last != nil {
		t.Fatalf("singleElement: queue first node or last node should be nil after clear.")
	}
}
