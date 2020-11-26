/*
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.general;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TCompactProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.storage.ExecResponse;
import com.vesoft.nebula.storage.GeneralStorageService;
import com.vesoft.nebula.storage.KVGetRequest;
import com.vesoft.nebula.storage.KVGetResponse;
import com.vesoft.nebula.storage.KVPutRequest;
import com.vesoft.nebula.storage.KVRemoveRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class GeneralStorageConnection {
    protected TTransport transport = null;
    protected TProtocol protocol = null;
    public HostAndPort address;
    private GeneralStorageService.Client generalClient;

    // 0: idle, 1: using
    private int stat = 0;

    public GeneralStorageConnection() {
    }

    protected GeneralStorageConnection open(HostAndPort address, int timeout) throws Exception {
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
            generalClient = new GeneralStorageService.Client(protocol);
        } catch (TException | UnknownHostException e) {
            throw e;
        }
        return this;
    }

    protected void close() {
        if (transport != null && transport.isOpen()) {
            transport.close();
        }
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

    public HostAndPort getAddress() {
        return address;
    }

    public boolean isBusy() {
        return stat == 1;
    }

    public void release() {
        this.stat = 0;
    }

    public void setBusy() {
        this.stat = 1;
    }
}
