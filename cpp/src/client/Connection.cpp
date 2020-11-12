/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "common/interface/gen-cpp2/GraphServiceAsyncClient.h"
#include "nebula/client/Connection.h"

namespace nebula {

Connection::Connection() : client_{nullptr} {}

Connection::~Connection() {
    delete client_;
}

Connection &Connection::operator=(Connection &&c) {
    delete client_;
    client_ = c.client_;
    c.client_ = nullptr;
    return *this;
}

bool Connection::open(const std::string &address, int32_t port) {
    try {
        auto socket = apache::thrift::async::TAsyncSocket::newSocket(
            folly::EventBaseManager::get()->getEventBase(), address, port, 0 /*TODO(shylock) pass from config*/);

        client_ = new graph::cpp2::GraphServiceAsyncClient(
            apache::thrift::HeaderClientChannel::newChannel(socket));
    } catch (const std::exception &) {
        return false;
    }
    return true;
}

AuthResponse Connection::authenticate(const std::string &user, const std::string &password) {
    if (client_ == nullptr) {
        return AuthResponse{
            ErrorCode::E_DISCONNECTED, nullptr, std::make_unique<std::string>("Not open connection.")};
    }

    AuthResponse resp;
    try {
        client_->sync_authenticate(resp, user, password);
    } catch (const std::exception &ex) {
        return AuthResponse{
            ErrorCode::E_RPC_FAILURE, nullptr, std::make_unique<std::string>("Unavailable Connection.")};
    }
    return resp;
}

ExecutionResponse Connection::execute(int64_t sessionId, const std::string &stmt) {
    if (client_ == nullptr) {
        return ExecutionResponse{ErrorCode::E_DISCONNECTED, 0};
    }

    ExecutionResponse resp;
    try {
        client_->sync_execute(resp, sessionId, stmt);
    } catch (const std::exception &ex) {
        return ExecutionResponse{ErrorCode::E_RPC_FAILURE, 0};
    }

    return resp;
}

void Connection::asyncExecute(int64_t sessionId, const std::string &stmt, ExecuteCallback cb) {
    if (client_ == nullptr) {
        cb(ExecutionResponse{ErrorCode::E_DISCONNECTED});
        return;
    }
    client_->future_execute(sessionId, stmt).thenValue([cb = std::move(cb)](auto &&resp) {
        cb(std::move(resp));
    });
}

std::string Connection::executeJson(int64_t sessionId, const std::string &stmt) {
    if (client_ == nullptr) {
        // TODO handle error
        return "";
    }

    std::string json;
    try {
        client_->sync_executeJson(json, sessionId, stmt);
    } catch (const std::exception &ex) {
        // TODO handle error
        return "";
    }

    return json;
}

void Connection::asyncExecuteJson(int64_t sessionId,
                                  const std::string &stmt,
                                  ExecuteJsonCallback cb) {
    if (client_ == nullptr) {
        cb("");
        return;
    }
    client_->future_executeJson(sessionId, stmt).thenValue(std::move(cb));
}

bool Connection::isOpen() {
    return ping();
}

void Connection::close() {
    if (client_ != nullptr) {
        static_cast<apache::thrift::ClientChannel *>(client_->getChannel())->closeNow();
    }
}

bool Connection::ping() {
    auto resp = execute(-1 /*Only check connection*/, "YIELD 1");
    if (resp.error_code == ErrorCode::E_RPC_FAILURE ||
        resp.error_code == ErrorCode::E_DISCONNECTED) {
        return false;
    }
    return true;
}

void Connection::signout(int64_t sessionId) {
    if (client_ != nullptr) {
        client_->sync_signout(sessionId);
    }
}

}   // namespace nebula
