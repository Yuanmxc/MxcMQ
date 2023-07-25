package queue

import (
	"container/list"
	"sync"
)

type Queue struct {
	mu   sync.Mutex
	list *list.List
}

func New() *Queue {
	return &Queue{list: list.New()}
}

func (queue *Queue) Push(value interface{}) {
	queue.mu.Lock()
	queue.list.PushBack(value)
	queue.mu.Unlock()
}

func (queue *Queue) Front() interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	it := queue.list.Front()
	if it != nil {
		return it.Value
	}
	return nil
}

func (queue *Queue) Back() interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	it := queue.list.Back()
	if it != nil {
		return it.Value
	}
	return nil
}

func (queue *Queue) Pop() interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	it := queue.list.Front()
	if it != nil {
		queue.list.Remove(it)
		return it.Value
	}
	return nil
}

func (queue *Queue) Size() int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	len := queue.list.Len()
	return len
}

func (queue *Queue) Empty() bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	len := queue.list.Len()
	return len == 0
}

func (queue *Queue) Clear() {
	for !queue.Empty() {
		queue.Pop()
	}
}
