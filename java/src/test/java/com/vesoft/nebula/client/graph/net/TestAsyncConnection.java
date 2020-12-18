package com.vesoft.nebula.client.graph.net;

import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.graph.AuthResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;


public class TestAsyncConnection {
    @Test
    public void testAll() {
        ASyncConnection connection = new ASyncConnection();
        try {
            // Test open
            try {
                connection.open(new HostAddress("127.0.0.1", 3699), 1000);
            } catch (IOErrorException e) {
                e.printStackTrace();
                assert false;
            }

            // Test authenticate
            RpcResponse<AuthResponse> resp = connection.authenticate("root", "nebula").get();
            assert !resp.hasError();
            long sessionId = resp.getResult().getSession_id();
            assert sessionId != 0;

            // Test execute with callback
            connection.execute(sessionId, "SHOW HOSTS",
                (RpcResponse<ResultSet> rpcResponse) -> {
                    ResultSet resultSet = rpcResponse.getResult();
                    List<String> colNames = resultSet.getColumnNames();
                    for (String name : colNames) {
                        System.out.printf("%15s |", name);
                    }
                    System.out.println();
                    for (ResultSet.Record record : resultSet.getRecords()) {
                        for (Value rec : record) {
                            Object value = rec.getFieldValue();
                            if (value instanceof Integer) {
                                System.out.print(String.format("%15s |", value));
                            }
                            if (value instanceof Long) {
                                System.out.print(String.format("%15s |", value));
                            }
                            if (value instanceof byte[]) {
                                System.out.print(String.format("%15s |",
                                                                new String((byte[]) value)));
                            }
                        }
                        System.out.println();
                    }
                });

            TimeUnit.SECONDS.sleep(2);

            // test with future
            RpcResponse<ResultSet> executeResult = connection.execute(sessionId,
                                                                 "SHOW HOSTS").get();
            assert !executeResult.hasError();
            ResultSet resultSet = executeResult.getResult();
            assert resultSet.isSucceeded();
            assert resultSet.getColumnNames().size() == 6;
            assert resultSet.getColumnNames().get(0) == "Host";
            assert resultSet.getColumnNames().get(1) == "Port";
            assert resultSet.getColumnNames().get(2) == "Status";
            assert resultSet.getColumnNames().get(3) == "Leader count";
            assert resultSet.getColumnNames().get(4) == "Leader distribution";
            assert resultSet.getColumnNames().get(5) == "Partition distribution";

            List<ResultSet.Record> records = resultSet.getRecords();
            assert new String((byte[])records.get(0).get(0).getFieldValue()).equals("storaged0");
            assert new String((byte[])records.get(1).get(0).getFieldValue()).equals("storaged1");
            assert new String((byte[])records.get(2).get(0).getFieldValue()).equals("storaged2");

            assert records.get(0).get(1).getIVal() == 44500;
            assert records.get(1).get(1).getIVal() == 44500;
            assert records.get(2).get(1).getIVal() == 44500;

            connection.signOut(sessionId);
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        } finally {
            connection.close();
        }
    }
}
