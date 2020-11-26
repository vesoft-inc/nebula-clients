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
    private final List<HostAndPort> metaAddresses;
    private MetaClient metaClient;

    public StorageConnPool(StoragePoolConfig config, List<HostAndPort> metaAddresses)
            throws TException {
        poolFactory = new StorageConnPoolFactory(config);

        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
        poolConfig.setMaxIdlePerKey(config.getMaxConnsSize());
        poolConfig.setMinIdlePerKey(config.getMinConnsSize());
        poolConfig.setMinEvictableIdleTimeMillis(
                config.getIdleTime() <= 0 ? Long.MAX_VALUE : config.getIdleTime());
        poolConfig.setMaxTotal(config.getMaxTotal());
        poolConfig.setMaxTotalPerKey(config.getMaxTotalPerKey());

        this.metaAddresses = metaAddresses;
        keyedPool = new GenericKeyedObjectPool<>(poolFactory);
        keyedPool.setConfig(poolConfig);
        metaClient = new MetaClient(metaAddresses);
        metaClient.connect();
    }

    public void close() {
        keyedPool.close();
    }

    public StorageConnection getStorageConnection(HostAndPort address) throws Exception {
        return keyedPool.borrowObject(address);
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

    public MetaClient getMetaClient() {
        return metaClient;
    }
}
