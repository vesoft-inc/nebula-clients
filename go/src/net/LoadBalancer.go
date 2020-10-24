/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

// TODO: add implement round-robin rule

package nebulaNet

import (
	"container/heap"

	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type ServerStatus struct {
	address     data.HostAddress
	isAvaliable bool
	currentLoad int
	index       int
}

// type LoadBalancer struct {
// 	addresses    *data.HostAddress
// 	ServerStatus map[*data.HostAddress]bool
// 	ServerStatus map[*data.HostAddress]int
// }

type PriorityQueue []*ServerStatus

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].currentLoad < pq[j].currentLoad
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*ServerStatus)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(serverStatus *ServerStatus, isAvaliable bool,
	currentLoad int, index int) {
	serverStatus.isAvaliable = isAvaliable
	serverStatus.currentLoad = currentLoad
	heap.Fix(pq, serverStatus.index)
}
