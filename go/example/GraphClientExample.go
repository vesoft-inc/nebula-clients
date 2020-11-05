/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	nebula "github.com/vesoft-inc/nebula-clients/go"
	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
)

const (
	address  = "127.0.0.1"
	port     = 3699
	username = "user"
	password = "password"
)

// Initialize logger
var log = nebula.DefaultLogger{}

func main() {
	hostAdress := nebula.HostAddress{Host: address, Port: port}
	hostList := []nebula.HostAddress{
		hostAdress,
	}
	pool := nebula.ConnectionPool{}

	// Create configs for connection pool using default values
	testPoolConfig := nebula.GetDefaultConf(log)
	// Initialize connectin pool
	err := pool.InitPool(hostList, testPoolConfig, log)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error()))
	}
	// Close all connections in the pool
	defer pool.Close()
	// Create session and send query in go routine
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		// Create session
		session, err := pool.GetSession(username, password)
		if err != nil {
			log.Fatal(fmt.Sprintf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
				username, password, err.Error()))
		}
		// Release session and return connection back to connection pool
		defer session.Release()
		// Method used to check execution response
		checkResp := func(prefix string, err *graph.ExecutionResponse) {
			if nebula.IsError(err) {
				fmt.Printf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, err.GetErrorCode(), err.GetErrorMsg())
			}
		}
		{
			createSchema := "CREATE SPACE IF NOT EXISTS test; " +
				"USE test;" +
				"CREATE TAG IF NOT EXISTS person(name string, age int);" +
				"CREATE EDGE IF NOT EXISTS like(likeness double)"

			// Excute a query
			resp, err := session.Execute(createSchema)
			if err != nil {
				fmt.Printf(err.Error())
				return
			}
			checkResp(createSchema, resp)
		}
		time.Sleep(5 * time.Second)
		{
			insertVertexes := "INSERT VERTEX person(name, age) VALUES " +
				"'Bob':('Bob', 10), " +
				"'Lily':('Lily', 9), " +
				"'Tom':('Tom', 10), " +
				"'Jerry':('Jerry', 13), " +
				"'John':('John', 11);"

			// Insert multiple vertexes
			resp, err := session.Execute(insertVertexes)
			if err != nil {
				fmt.Printf(err.Error())
				return
			}
			checkResp(insertVertexes, resp)
		}

		{
			// Insert multiple edges
			insertEdges := "INSERT EDGE like(likeness) VALUES " +
				"'Bob'->'Lily':(80.0), " +
				"'Bob'->'Tom':(70.0), " +
				"'Lily'->'Jerry':(84.0), " +
				"'Tom'->'Jerry':(68.3), " +
				"'Bob'->'John':(97.2);"

			resp, err := session.Execute(insertEdges)
			if err != nil {
				fmt.Printf(err.Error())
				return
			}
			checkResp(insertEdges, resp)
		}

		{
			query := "GO FROM 'Bob' OVER like YIELD $^.person.name, $^.person.age, like.likeness"
			// Send query
			resp, err := session.Execute(query)
			if err != nil {
				fmt.Printf(err.Error())
				return
			}
			checkResp(query, resp)
			printResult(resp)
		}
	}(&wg)

	wg.Wait()

	log.Info("Example finished")
}

func printResult(resp *graph.ExecutionResponse) {
	data := resp.GetData()
	colNames := data.GetColumnNames()
	for _, col := range colNames {
		fmt.Printf("%15s |", col)
	}
	fmt.Println()
	rows := data.GetRows()
	for _, row := range rows {
		values := row.GetValues()
		for _, value := range values {
			if value.IsSetNVal() {
				fmt.Printf("%15s |", "__NULL__")
			} else if value.IsSetBVal() {
				fmt.Printf("%15t |", strconv.FormatBool(value.GetBVal()))
			} else if value.IsSetIVal() {
				fmt.Printf("%15d |", value.GetIVal())
			} else if value.IsSetFVal() {
				fmt.Printf("%15.1f |", value.GetFVal())
			} else if value.IsSetSVal() {
				fmt.Printf("%15s |", value.GetSVal())
			} else if value.IsSetDVal() {
				fmt.Printf("%15s |", value.GetDVal())
			} else if value.IsSetTVal() {
				fmt.Printf("%15s |", value.GetTVal())
			} else if value.IsSetDtVal() {
				fmt.Printf("%15s |", value.GetDtVal())
			} else if value.IsSetVVal() {
				fmt.Printf("%15s |", value.GetVVal())
			} else if value.IsSetEVal() {
				fmt.Printf("%15s |", value.GetEVal())
			} else if value.IsSetPVal() {
				fmt.Printf("%15s |", value.GetPVal())
			} else if value.IsSetLVal() {
				fmt.Printf("%15s |", value.GetLVal())
			} else if value.IsSetMVal() {
				fmt.Printf("%15s |", value.GetMVal())
			} else if value.IsSetUVal() {
				fmt.Printf("%15s |", value.GetUVal())
			}
		}
		fmt.Println()
	}
}
