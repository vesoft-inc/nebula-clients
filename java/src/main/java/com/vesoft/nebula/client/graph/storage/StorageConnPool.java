/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import java.util.List;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageConnPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageConnPool.class);

    private GenericKeyedObjectPool<HostAndPort, StorageConnection> keyedPool;
    private final StorageConnPoolFactory poolFactory;
    private MetaClient metaClient;

    public StorageConnPool(NebulaPoolConfig config, List<HostAndPort> metaAddresses)
            throws TException {
        poolFactory = new StorageConnPoolFactory(config);

        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
        poolConfig.setMaxIdlePerKey(config.getMaxConnSize());
        poolConfig.setMinIdlePerKey(config.getMinConnSize());
        poolConfig.setMinEvictableIdleTimeMillis(
                config.getIdleTime() <= 0 ? Long.MAX_VALUE : config.getIdleTime());
        poolConfig.setMaxTotal(config.getMaxTotal());
        poolConfig.setMaxTotalPerKey(config.getMaxTotalPerKey());

        keyedPool = new GenericKeyedObjectPool<>(poolFactory);
        keyedPool.setConfig(poolConfig);
        metaClient = new MetaClient(metaAddresses);
        metaClient.connect();
    }

    public void close() {
        keyedPool.close();
    }

    public StorageClient getStorageClient(HostAndPort address) throws Exception {
        StorageConnection connection = keyedPool.borrowObject(address);
        return new StorageClient(this, connection, metaClient);
    }

    public GeneralStorageClient getGeneralStorageClient(HostAndPort address) throws Exception {
        StorageConnection connection = keyedPool.borrowObject(address);
        return new GeneralStorageClient(this, connection, metaClient);
    }

    public void release(HostAndPort address, StorageConnection connection) {
        keyedPool.returnObject(address, connection);
    }

    public int getNumActive(HostAndPort address) {
        return keyedPool.getNumActive(address);
    }

    public int get(HostAndPort address) {
        return keyedPool.getNumIdle(address);
    }
}
