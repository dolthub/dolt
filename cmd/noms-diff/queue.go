package main

import (
	"github.com/attic-labs/noms/types"
)

type diffInfo struct {
	path types.Path
	key  types.Value
	v1   types.Value
	v2   types.Value
}

type queueNode struct {
	value diffInfo
	next  *queueNode
}

type diffQueue struct {
	head *queueNode
	tail *queueNode
	len  int
}

func (q *diffQueue) Push(node diffInfo) {
	qn := queueNode{value: node}
	q.len += 1
	if q.head == nil {
		q.head = &qn
		q.tail = &qn
		return
	}
	q.tail.next = &qn
	q.tail = &qn
}

func (q *diffQueue) Pop() (diffInfo, bool) {
	if q.head == nil {
		return diffInfo{}, false
	}
	q.len -= 1
	qn := q.head
	if q.head == q.tail {
		q.head = nil
		q.tail = nil
	}

	q.head = qn.next
	return qn.value, true
}
