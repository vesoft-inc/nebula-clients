/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.meta;

import com.facebook.thrift.TException;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.MockNebula;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ExecuteFailedException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.meta.ErrorCode;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.SpaceItem;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;

public class MetaClientTest extends TestCase {

    private final String address = "127.0.0.1";
    private final int port = 45500;
    private final String space = "test";

    private MetaClient metaClient;
    private Session session;


    public void setUp() throws Exception {
        MockNebula.createGraph();
        testConnect();
    }

    public void tearDown() throws Exception {
    }

    public void testConnect() {
        metaClient = new MetaClient(Arrays.asList(HostAndPort.fromParts(address, port)));
        try {
            int res = metaClient.connect();
            assertTrue(res == ErrorCode.SUCCEEDED);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public void testMetaInfo() {
        MetaInfo metaInfo = metaClient.getMetaInfo();
        assert (metaInfo.getSpaceNameMap().containsKey(space));
    }

    public void testGetSpaces() {
        Map<String, Integer> spaces = metaClient.getSpaces();
        assert (spaces.keySet().contains(space));
    }

    public void testGetSpace() {
        try {
            SpaceItem spaceItem = metaClient.getSpace(space);
        } catch (TException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testGetTags() {
        try {
            assert (metaClient.getTags(space).size() == 1);
        } catch (TException | ExecuteFailedException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testGetTag() {
        try {
            Schema schema = metaClient.getTag(space, "person");
        } catch (TException | ExecuteFailedException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testGetEdges() {
        try {
            assert (metaClient.getEdges(space).size() == 1);
        } catch (TException | ExecuteFailedException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testGetEdge() {
        try {
            Schema schema = metaClient.getEdge(space, "like");
        } catch (TException | ExecuteFailedException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testGetPartsAlloc() {
        try {
            assert (metaClient.getPartsAlloc(space).size() == 100);
        } catch (ExecuteFailedException | TException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testFreshMetaInfo() {
        try {
            metaClient.freshMetaInfo();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testFreshMeta() throws InterruptedException {
        try {
            getSession();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        metaClient.getSpaceId(space);

        try {
            session.execute("CREATE SPACE nebula");
        } catch (IOErrorException e) {
            e.printStackTrace();
            fail();
        }
        Thread.sleep(1000);
        metaClient.getLeader("nebula", 1);
        assert (metaClient.getSpaceId("nebula") > 0);
        assert (metaClient.getMetaInfo().getLeaders().containsKey("nebula"));
        assert (metaClient.getMetaInfo().getSpaceTagItems().containsKey("nebula"));
        assert (metaClient.getMetaInfo().getSpaceEdgeItems().containsKey("nebula"));
    }

    public void getSession() throws NotValidConnectionException, IOErrorException,
            AuthFailedException, UnknownHostException {
        List<HostAddress> addresses = Collections.singletonList(
                new HostAddress("127.0.0.1", 3699));
        NebulaPool pool = new NebulaPool();

        pool.init(addresses, new NebulaPoolConfig());

        session = pool.getSession("root", "nebula", true);
    }

}