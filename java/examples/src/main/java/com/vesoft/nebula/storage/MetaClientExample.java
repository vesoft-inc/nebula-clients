/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.storage;

import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.meta.MetaClient;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClientExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaClientExample.class);

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: com.vesoft.nebula.examples.MetaClientExample"
                    + "<meta_server_ip> <meta_server_port");
            return;
        }

        try {
            List<HostAndPort> address = Arrays.asList(HostAndPort.fromParts(args[0],
                    Integer.parseInt(args[1])));
            MetaClient metaClient = new MetaClient(address);
            metaClient.connect();
            LOGGER.info(metaClient.getSpaces().toString());
            LOGGER.info(metaClient.getSpace("test").properties.toString());
            LOGGER.info(metaClient.getTags("test").toString());
            LOGGER.info(metaClient.getTag("test", "test_tag").toString());
            LOGGER.info(metaClient.getEdges("test").toString());
            LOGGER.info(metaClient.getEdge("test", "test_edge").toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
