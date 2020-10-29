/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/vesoft-inc/nebula-clients/go/nebula/graph"
	"github.com/vesoft-inc/nebula-clients/go/src/conf"
	data "github.com/vesoft-inc/nebula-clients/go/src/data"
	nebulaNet "github.com/vesoft-inc/nebula-clients/go/src/net"
)

const (
	address  = "127.0.0.1"
	port     = 3699
	username = "user"
	password = "password"
)

func main() {
	hostAdress := data.NewHostAddress(address, port)
	hostList := []*data.HostAddress{}
	hostList = append(hostList, &hostAdress)
	pool := nebulaNet.ConnectionPool{}

	// Create configs for connection pool using default values
	testPoolConfig := conf.GetDefaultConf()
	// Initialize connectin pool
	err := pool.InitPool(hostList, &testPoolConfig)
	if err != nil {
		fmt.Printf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error())
	}
	// Create session
	session, err := pool.GetSession(username, password)
	if err != nil {
		fmt.Printf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			username, password, err.Error())
	}
	// Method used to check execution response
	checkResp := func(prefix string, err *graph.ExecutionResponse) {
		if nebulaNet.IsError(err) {
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

		// Create a new space
		resp, err := session.Execute(insertVertexes)
		if err != nil {
			fmt.Printf(err.Error())
			return
		}
		checkResp(insertVertexes, resp)
	}

	{
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
		resp, err := session.Execute(query)
		if err != nil {
			fmt.Printf(err.Error())
			return
		}
		checkResp(query, resp)
		printResult(resp)
	}
	// Release session and return connection back to connection pool
	session.Release()
	// Close all connections in the pool
	pool.Close()

	fmt.Println("Example finished")
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
