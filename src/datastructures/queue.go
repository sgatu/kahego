package datastructures

import "errors"

type Queue[T interface{}] struct {
	first *Node[T]
	last  *Node[T]
	len   uint32
}
type Node[T interface{}] struct {
	Value T
	next  *Node[T]
}

func NewQueue[T interface{}]() *Queue[T] {
	return &Queue[T]{len: 0}
}
func (queue *Queue[T]) Push(node *Node[T]) {
	if queue.first == nil {
		queue.first = node
		queue.last = node
		queue.len++
		return
	}
	last := queue.last
	last.next = node
	queue.last = node
	queue.len++
}
func (queue *Queue[T]) Pop() (*Node[T], error) {
	if queue.len == 0 || queue.first == nil {
		return nil, errors.New("empty queue")
	}
	retVal := queue.first
	queue.first = retVal.next
	queue.len--
	return retVal, nil
}
func (queue *Queue[T]) Len() uint32 {
	return queue.len
}
