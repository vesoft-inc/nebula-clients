/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
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


struct ProfilingStats {
    // How many rows being processed in an executor.
    int64_t rows;
    // Duration spent in an executor.
    int64_t exec_duration_in_us;
    // Total duration spent in an executor, contains schedule time
    int64_t total_duration_in_us;
    // Other profiling stats data map
    std::unique_ptr<std::unordered_map<std::string, std::string>> other_stats;
};

// The info used for select/loop.
struct PlanNodeBranchInfo {
    // True if loop body or then branch of select
    bool  is_do_branch;
    // select/loop node id
    int64_t  condition_node_id;
};

struct Pair {
    std::string key;
    std::string value;
};

struct PlanNodeDescription {
    std::string                                   name;
    int64_t                                       id;
    std::string                                   output_var;
    // other description of an executor
    std::unique_ptr<std::vector<Pair>>            description;
    // If an executor would be executed multi times,
    // the profiling statistics should be multi-versioned.
    std::unique_ptr<std::vector<ProfilingStats>>   profiles;
    std::unique_ptr<PlanNodeBranchInfo>            branch_info;
    std::unique_ptr<std::vector<int64_t>>          dependencies;
};

struct PlanDescription {
    std::vector<PlanNodeDescription>     plan_node_descs;
    // map from node id to index of list
    std::unordered_map<int64_t, int64_t> node_index_map;
    // the print format of exec plan, lowercase string like `dot'
    std::string                          format;
};

struct ExecutionResponse {
    ErrorCode code{ErrorCode::E_UNKNOWN};
    int32_t latencyInUs{0};
    std::unique_ptr<nebula::DataSet> data{nullptr};
    std::unique_ptr<std::string> space_name{nullptr};
    std::unique_ptr<std::string> error_msg{nullptr};
    std::unique_ptr<PlanDescription> plan_desc{nullptr};
    std::unique_ptr<std::string> comment{nullptr};
};

}   // namespace nebula
