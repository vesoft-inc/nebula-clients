/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/datatypes/DataSet.h"

namespace nebula {

enum class ErrorCode {
    SUCCEEDED = 0,
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,
    E_BAD_USERNAME_PASSWORD = -4,
    E_SESSION_INVALID = -5,
    E_SESSION_TIMEOUT = -6,
    E_SYNTAX_ERROR = -7,
    E_EXECUTION_ERROR = -8,
    E_STATEMENT_EMTPY = -9,
    E_USER_NOT_FOUND = -10,
    E_BAD_PERMISSION = -11,
    E_SEMANTIC_ERROR = -12,
    E_UNKNOWN = -13,
};

struct AuthResponse {
    ErrorCode code{ErrorCode::E_UNKNOWN};
    int64_t sessionId{-1};
};

struct ExecutionResponse {
    ErrorCode code{ErrorCode::E_UNKNOWN};
    int32_t latencyInUs{0};
    std::unique_ptr<nebula::DataSet> data{nullptr};
    std::unique_ptr<std::string> space_name{nullptr};
    std::unique_ptr<std::string> error_msg{nullptr};
    // TODO(shylock)
    //    ::nebula::graph::cpp2::PlanDescription plan_desc;
    std::unique_ptr<std::string> comment{nullptr};
};

}   // namespace nebula
