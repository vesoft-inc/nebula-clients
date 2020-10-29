/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

// TODO: add implement round-robin rule

package nebulaNet

import (
	"errors"
	"log"
	"sort"
	"sync"

	conf "github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
)

type ServerStatus struct {
	address  *data.HostAddress
	isValid  bool
	workLoad int
}

type LoadBalancer struct {
	pool             *ConnectionPool
	ServerStatusList []*ServerStatus
	RWlock           sync.RWMutex
}

func NewLoadbalancer(addresses []*data.HostAddress, pool *ConnectionPool) *LoadBalancer {
	newLoadBalancer := LoadBalancer{}
	newLoadBalancer.pool = pool
	for _, host := range addresses {
		newServerStatus := ServerStatus{}
		newServerStatus.address = host
		newServerStatus.isValid = false
		newServerStatus.workLoad = 0
		newLoadBalancer.ServerStatusList = append(newLoadBalancer.ServerStatusList, &newServerStatus)
	}
	return &newLoadBalancer
}

// Test if the host is valid
func PingHost(hostAddress *data.HostAddress) (bool, error) {
	conn := NewConnection(*hostAddress)
	conf := conf.GetDefaultConf()
	err := conn.Open(*hostAddress, conf)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return false, err
	}
	conn.Close()
	return true, nil
}

// Update all server's status (validility)
func (loadBalancer *LoadBalancer) UpdateServerStatus() {
	loadBalancer.RWlock.Lock()
	defer loadBalancer.RWlock.Unlock()
	for _, status := range loadBalancer.ServerStatusList {
		_, err := PingHost(status.address)
		if err != nil {
			log.Printf("Error: %s", err.Error())
			status.isValid = false
			status.address.IsAvaliable = false
			continue
		}
		status.isValid = true
	}
}

// Sort host by workload
func (loadBalancer *LoadBalancer) SortHosts() {
	loadBalancer.RWlock.Lock()
	defer loadBalancer.RWlock.Unlock()
	sort.Slice(loadBalancer.ServerStatusList, func(i, j int) bool {
		return loadBalancer.ServerStatusList[i].workLoad < loadBalancer.ServerStatusList[j].workLoad
	})
}

// Returns the first host in the host list that is valid
// TODO: add test for worklaod
func (loadBalancer *LoadBalancer) GetValidHost() (*data.HostAddress, error) {
	loadBalancer.SortHosts()
	loadBalancer.RWlock.RLock()
	defer loadBalancer.RWlock.RUnlock()
	for _, status := range loadBalancer.ServerStatusList {
		if status.isValid == true {
			return status.address, nil
		}
	}
	noAvaliableHost := errors.New("Failed to find an avaliable host in the connection pool")
	log.Print(noAvaliableHost)
	return nil, noAvaliableHost
}

// Mark the host as valid
func (loadBalancer *LoadBalancer) ValidateHost(SeverAddress *data.HostAddress) {
	loadBalancer.RWlock.Lock()
	defer loadBalancer.RWlock.Unlock()
	for _, status := range loadBalancer.ServerStatusList {
		if *status.address == *SeverAddress {
			status.isValid = true
		}
	}
}

// Increase Workload
func (loadBalancer *LoadBalancer) IncreaseWorkload(SeverAddress *data.HostAddress) {
	loadBalancer.RWlock.Lock()
	defer loadBalancer.RWlock.Unlock()
	for _, status := range loadBalancer.ServerStatusList {
		if *status.address == *SeverAddress {
			status.workLoad = status.workLoad + 1
		}
	}
}

// Decrease Workload
func (loadBalancer *LoadBalancer) DecreaseWorkload(SeverAddress *data.HostAddress) {
	loadBalancer.RWlock.Lock()
	defer loadBalancer.RWlock.Unlock()
	for _, status := range loadBalancer.ServerStatusList {
		if *status.address == *SeverAddress {
			status.workLoad = status.workLoad - 1
		}
	}
}

// TODO: Add timer to track idle time of connections and release them
func (loadBalancer *LoadBalancer) ClearIdleConn(SeverAddress *data.HostAddress) {
}
