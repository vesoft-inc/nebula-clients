/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.net;


import com.facebook.thrift.TException;
import com.facebook.thrift.async.AsyncMethodCallback;
import com.facebook.thrift.async.TAsyncClientManager;
import com.facebook.thrift.async.TAsyncMethodCall;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocolFactory;
import com.facebook.thrift.transport.TNonblockingSocket;
import com.facebook.thrift.transport.TNonblockingTransport;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.graph.AuthResponse;
import com.vesoft.nebula.graph.ExecutionResponse;
import com.vesoft.nebula.graph.GraphService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ASyncConnection extends Connection {
    protected TNonblockingTransport transport = null;
    private GraphService.AsyncClient client = null;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void open(HostAddress address, int timeout) throws IOErrorException {
        this.serverAddr = address;
        try {
            TAsyncClientManager clientManager = new TAsyncClientManager();
            transport = new TNonblockingSocket(address.getHost(), address.getPort());
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            client = new GraphService.AsyncClient(protocolFactory, clientManager, transport);
        } catch (Exception e) {
            throw new IOErrorException(IOErrorException.E_CONNECT_BROKEN, e.getMessage());
        }
    }

    public NFuture<RpcResponse<AuthResponse>> authenticate(String user, String password) {
        NFuture<RpcResponse<AuthResponse>> authResp = new NFuture<>();
        try {
            client.authenticate(user.getBytes(), password.getBytes(), new AsyncMethodCallback() {
                @Override
                public void onComplete(TAsyncMethodCall response) {
                    if (response instanceof GraphService.AsyncClient.authenticate_call) {
                        try {
                            AuthResponse result =
                                    ((GraphService.AsyncClient.authenticate_call) response)
                                            .getResult();
                            RpcResponse<AuthResponse> rpcResponse = new RpcResponse<>(result);
                            authResp.setResult(rpcResponse);
                        } catch (TException e) {
                            log.error(String.format("Authenticate get exception: %s",
                                                     e.getMessage()));
                            RpcResponse<AuthResponse> rpcResponse = new RpcResponse<>(
                                    new IOErrorException(IOErrorException.E_UNKNOWN,
                                                         e.getMessage()));
                            authResp.setResult(rpcResponse);
                        }
                    }
                }

                @Override
                public void onError(Exception e) {
                    log.error(String.format("Authenticate get exception: %s", e.getMessage()));
                    RpcResponse<AuthResponse> rpcResponse = new RpcResponse<>(
                            new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage()));
                    authResp.setResult(rpcResponse);
                }
            });
        } catch (Exception e) {
            log.error(String.format("Authenticate get exception: %s", e.getMessage()));
            RpcResponse<AuthResponse> rpcResponse = new RpcResponse<>(
                    new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage()));
            authResp.setResult(rpcResponse);
        }
        return authResp;
    }

    public void execute(long sessionId, String stmt, Consumer<RpcResponse<ResultSet>> callback) {
        try {
            client.execute(sessionId, stmt.getBytes(), new AsyncMethodCallback() {
                @Override
                public void onComplete(TAsyncMethodCall response) {
                    if (response instanceof GraphService.AsyncClient.execute_call) {
                        try {
                            ExecutionResponse result =
                                    ((GraphService.AsyncClient.execute_call) response)
                                            .getResult();
                            RpcResponse<ResultSet> rpcResponse =
                                    new RpcResponse<>(new ResultSet(result));
                            callback.accept(rpcResponse);
                        } catch (TException e) {
                            RpcResponse<ResultSet> rpcResponse = new RpcResponse<>(
                                    new IOErrorException(IOErrorException.E_UNKNOWN,
                                                         e.getMessage()));
                            callback.accept(rpcResponse);
                            log.error(String.format("Execute get exception: %s", e.getMessage()));
                        }
                    }
                }

                @Override
                public void onError(Exception e) {
                    RpcResponse<ResultSet> rpcResponse = new RpcResponse<>(
                            new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage()));
                    callback.accept(rpcResponse);
                    log.error(String.format("Execute get exception: %s", e.getMessage()));
                }
            });
        } catch (Exception e) {
            log.error(String.format("Execute get exception: %s", e.getMessage()));
            RpcResponse<ResultSet> rpcResponse = new RpcResponse<>(
                    new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage()));
            callback.accept(rpcResponse);
        }
    }

    public NFuture<RpcResponse<ResultSet>> execute(long sessionId, String stmt) {
        NFuture<RpcResponse<ResultSet>> executeResp = new NFuture<>();
        try {
            client.execute(sessionId, stmt.getBytes(), new AsyncMethodCallback() {
                @Override
                public void onComplete(TAsyncMethodCall response) {
                    if (response instanceof GraphService.AsyncClient.execute_call) {
                        try {
                            ExecutionResponse result =
                                    ((GraphService.AsyncClient.execute_call) response)
                                            .getResult();
                            RpcResponse<ResultSet> rpcResponse =
                                    new RpcResponse<>(new ResultSet(result));
                            executeResp.setResult(rpcResponse);
                        } catch (TException e) {
                            log.error(String.format("Execute get exception: %s", e.getMessage()));
                        }
                    } else {
                        RpcResponse<ResultSet> rpcResponse = new RpcResponse<>(
                                new IOErrorException(IOErrorException.E_UNKNOWN,
                                        String.format("Invalid result format : %s",
                                                response.toString())));
                        executeResp.setResult(rpcResponse);
                    }
                }

                @Override
                public void onError(Exception e) {
                    log.error(String.format("Execute get exception: %s", e.getMessage()));
                    RpcResponse<ResultSet> rpcResponse = new RpcResponse<>(
                            new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage()));
                    executeResp.setResult(rpcResponse);
                }
            });
        } catch (Exception e) {
            log.error(String.format("Execute get exception: %s", e.getMessage()));
            RpcResponse<ResultSet> rpcResponse = new RpcResponse<>(
                    new IOErrorException(IOErrorException.E_UNKNOWN, e.getMessage()));
            executeResp.setResult(rpcResponse);
        }
        return executeResp;
    }


    public void signOut(long sessionId) {
        try {
            client.signout(sessionId, new AsyncMethodCallback() {
                @Override
                public void onComplete(TAsyncMethodCall response) {
                    return;
                }

                @Override
                public void onError(Exception exception) {
                    log.warn(String.format("SessionId [%d] signout failed", sessionId));
                }
            });
        } catch (TException e) {
            log.warn(String.format("SessionId [%d] signout failed", sessionId));
        }
    }

    @Override
    public void close() {
        if (transport != null) {
            transport.close();
        }
    }

    @Override
    public boolean ping() {
        try {
            RpcResponse<ResultSet> resp = this.execute(0, "YIELD 1;").get();
            if (resp.hasError()) {
                log.debug(String.format("ping addr [%s:%d] failed: %s",
                                        resp.getException().getMessage()));
                return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
