/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.net;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaPool {
    private GenericObjectPool<SyncConnection> objectPool = null;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public boolean init(List<HostAddress> addresses, NebulaPoolConfig config)
            throws UnknownHostException {
        List<HostAddress> newAddrs = hostToIp(addresses);
        ConnObjectPool objectPool = new ConnObjectPool(newAddrs, config);
        this.objectPool = new GenericObjectPool<>(objectPool);
        GenericObjectPoolConfig objConfig = new GenericObjectPoolConfig();
        objConfig.setMaxIdle(config.getIdleTime());
        objConfig.setMaxTotal(config.getMaxConnSize());
        this.objectPool.setConfig(objConfig);

        AbandonedConfig abandonedConfig = new AbandonedConfig();
        abandonedConfig.setRemoveAbandonedOnBorrow(true);
        this.objectPool.setAbandonedConfig(abandonedConfig);
        return objectPool.init();
    }

    public void close() {
        this.objectPool.close();
    }

    public Session getSession(String userName, String password, boolean reconnect)
            throws NotValidConnectionException, IOErrorException, AuthFailedException {
        try {
            SyncConnection connection = objectPool.borrowObject(1000);
            if (connection == null) {
                throw new NotValidConnectionException("Get connection object failed.");
            }
            log.info(String.format("Get connection to %s:%d",
                    connection.getServerAddress().getHost(),
                    connection.getServerAddress().getPort()));
            Session session = new Session(connection, this.objectPool, reconnect);
            session.auth(userName, password);
            return session;
        } catch (NotValidConnectionException | AuthFailedException | IOErrorException e) {
            throw e;
        } catch (IllegalStateException e) {
            throw new NotValidConnectionException(e.getMessage());
        } catch (Exception e) {
            throw new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage());
        }
    }

    public int getActiveConnNum() {
        return this.objectPool.getNumActive();
    }

    public int getIdleConnNum() {
        return this.objectPool.getNumIdle();
    }

    public int getWaitersNum() {
        return this.objectPool.getNumWaiters();
    }

    public void updateServerStatus() {
        if (this.objectPool.getFactory() instanceof ConnObjectPool) {
            ((ConnObjectPool)this.objectPool.getFactory()).updateServerStatus();
        }
    }

    private List<HostAddress> hostToIp(List<HostAddress> addresses)
            throws UnknownHostException {
        List<HostAddress> newAddrs = new ArrayList<HostAddress>();
        for (HostAddress addr : addresses) {
            String ip = InetAddress.getByName(addr.getHost()).getHostAddress();
            newAddrs.add(new HostAddress(ip, addr.getPort()));
        }
        return newAddrs;
    }
}
