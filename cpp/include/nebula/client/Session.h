/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "nebula/client/Connection.h"
#include "nebula/data/ResultSet.h"

namespace nebula {

class ConnectionPool;

class Session {
public:
    using ExecuteCallback = std::function<void(ResultSet &&)>;
    using ExecuteJsonCallback = std::function<void(std::string &&)>;

    Session() = default;
    Session(int64_t sessionId, Connection &&conn, ConnectionPool *pool)
        : sessionId_(sessionId), conn_(std::move(conn)), pool_(pool) {}
    Session(Session &&session)
        : sessionId_(session.sessionId_), conn_(std::move(session.conn_)), pool_(session.pool_) {
        session.sessionId_ = -1;
        session.pool_ = nullptr;
    }
    ~Session() {
        release();
    }

    ResultSet execute(const std::string &stmt);

    void asyncExecute(const std::string &stmt, ExecuteCallback cb);

    std::string executeJson(const std::string &stmt);

    void asyncExecuteJson(const std::string &stmt, ExecuteJsonCallback cb);

    bool ping();

    ErrorCode retryConnect();

    void release();

    bool valid() const {
        return sessionId_ > 0;
    }

    int64_t sessionId() const {
        return sessionId_;
    }

private:
    int64_t sessionId_{-1};
    Connection conn_;
    ConnectionPool *pool_{nullptr};
};

}   // namespace nebula
