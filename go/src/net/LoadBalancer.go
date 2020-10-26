/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

// TODO: add implement round-robin rule

package nebulaNet

import (
	"container/heap"
	"errors"
	"log"

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

// Update status of server's avaliability when reconncect
func (pool *ConnectionPool) UpdateStatus(curSession *Session) error {
	if pool == nil {
		invalidPoolError := errors.New("Failed to update host status, pool is invalid")
		log.Print(invalidPoolError)
		return invalidPoolError
	}
	for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value.(*Connection).SeverAddress == curSession.connection.SeverAddress {
			ele.Value.(*Connection).SeverAddress.IsAvaliable = false
		}
	}
	return nil
}

// Returns the first host in the host list that is valid
func (pool *ConnectionPool) GetValidHost() (*data.HostAddress, error) {
	for _, host := range pool.addresses {
		if host.IsAvaliable == true {
			return host, nil
		}
	}
	noAvaliableHost := errors.New("Failed to find an avaliable host in the connection pool")
	log.Print(noAvaliableHost)
	return nil, noAvaliableHost
}

// Returns the first connection in the idle connection queue that has a valid host
func (pool *ConnectionPool) GetValidConn() (*Connection, error) {
	for ele := pool.idleConnectionQueue.Front(); ele != nil; ele = ele.Next() {
		if ele.Value.(*Connection).SeverAddress.IsAvaliable == true {
			return ele.Value.(*Connection), nil
		}
	}
	noAvaliableConnectionErr := errors.New("Failed to reconnect: no avaliable connection to a different host can be found")
	return nil, noAvaliableConnectionErr
}
