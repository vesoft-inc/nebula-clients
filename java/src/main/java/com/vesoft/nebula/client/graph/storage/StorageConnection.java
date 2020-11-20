/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TCompactProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.storage.ExecResponse;
import com.vesoft.nebula.storage.GeneralStorageService;
import com.vesoft.nebula.storage.GraphStorageService;
import com.vesoft.nebula.storage.KVGetRequest;
import com.vesoft.nebula.storage.KVGetResponse;
import com.vesoft.nebula.storage.KVPutRequest;
import com.vesoft.nebula.storage.KVRemoveRequest;
import com.vesoft.nebula.storage.ScanEdgeRequest;
import com.vesoft.nebula.storage.ScanEdgeResponse;
import com.vesoft.nebula.storage.ScanVertexRequest;
import com.vesoft.nebula.storage.ScanVertexResponse;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class StorageConnection {
    protected TTransport transport = null;
    protected TProtocol protocol = null;
    public HostAndPort address;
    private GraphStorageService.Client client;
    private GeneralStorageService.Client generalClient;


    protected StorageConnection() {
    }

    protected StorageConnection open(HostAndPort address, int timeout) throws Exception {
        this.address = address;
        try {
            int newTimeout = timeout <= 0 ? Integer.MAX_VALUE : timeout;
            this.transport = new TSocket(
                    InetAddress.getByName(address.getHostText()).getHostAddress(),
                    address.getPort(),
                    newTimeout,
                    newTimeout);
            this.transport.open();
            this.protocol = new TCompactProtocol(transport);
            client = new GraphStorageService.Client(protocol);
            generalClient = new GeneralStorageService.Client(protocol);
        } catch (TException | UnknownHostException e) {
            throw e;
        }
        return this;
    }


    // Graph operations
    protected ExecResponse put(KVPutRequest request) throws TException {
        return generalClient.put(request);
    }


    protected KVGetResponse get(KVGetRequest request) throws TException {
        return generalClient.get(request);
    }


    protected ExecResponse remove(KVRemoveRequest request) throws TException {
        return generalClient.remove(request);
    }

    public ScanVertexResponse scanVertex(ScanVertexRequest request) throws TException {
        return client.scanVertex(request);
    }

    public ScanEdgeResponse scanEdge(ScanEdgeRequest request) throws TException {
        return client.scanEdge(request);
    }

    public void close() {
        if (transport != null) {
            transport.close();
        }
    }

    public HostAndPort getAddress() {
        return address;
    }

}
