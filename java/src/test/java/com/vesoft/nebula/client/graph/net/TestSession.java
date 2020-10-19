/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.net;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSession {
    private final Logger log = LoggerFactory.getLogger(TestSession.class);

    @Test()
    public void testReconnect() {
        NebulaPool pool = new NebulaPool();
        try {
            NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
            nebulaPoolConfig.setMinConnSize(2);
            nebulaPoolConfig.setMaxConnSize(2);
            nebulaPoolConfig.setIdleTime(2);
            List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 3699),
                    new HostAddress("127.0.0.1", 3700));
            assert pool.init(addresses, nebulaPoolConfig);
            Session session = pool.getSession("root", "nebula", false);
            System.out.println("==================================");
            // TODO: Add a task to stop the graphd("127.0.0.1:3700") after 10 second

            // TODO: Add a task to start the graphd("127.0.0.1:3700") after 20 second
            for (int i = 0; i < 20; i++) {
                try {
                    ResultSet resp = session.execute("SHOW SPACES");
                    if (!resp.isSucceeded()) {
                        log.error(String.format("Execute `SHOW SPACES' failed: %s",
                                resp.getErrorMessage()));
                    }
                } catch (IOErrorException ie) {
                    if (ie.getType() == IOErrorException.E_CONNECT_BROKEN) {
                        session = pool.getSession("root", "nebula", false);
                        session.execute("USE test");
                    }
                }
                TimeUnit.SECONDS.sleep(2);
            }
            session.release();
            Session session1 = pool.getSession("root", "nebula", false);
            assert (session1 != null);
            Session session2 = pool.getSession("root", "nebula", false);
            assert (session2 != null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertFalse(e.getMessage(),false);
        } finally {
            if (pool != null) {
                pool.close();
            }
        }
    }
}
