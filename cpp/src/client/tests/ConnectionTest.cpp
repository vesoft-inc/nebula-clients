/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/synchronization/Baton.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <nebula/client/Connection.h>
#include <nebula/client/Init.h>

#include "./ClientTest.h"

// Require a nebula server could access

class ConnectionTest : public ClientTest {
protected:
    static void runOnce(nebula::Connection &c) {
        // ping
        EXPECT_FALSE(c.ping());

        // execute
        auto resp = c.execute(-1, "SHOW SPACES");
        ASSERT_TRUE(resp.error_code == nebula::ErrorCode::E_DISCONNECTED ||
                    resp.error_code == nebula::ErrorCode::E_RPC_FAILURE);
        EXPECT_EQ(resp.data, nullptr);

        // async execute
        c.asyncExecute(-1, "SHOW SPACES", [](auto &&cbResp) {
            ASSERT_TRUE(cbResp.error_code == nebula::ErrorCode::E_DISCONNECTED ||
                        cbResp.error_code == nebula::ErrorCode::E_RPC_FAILURE);
            EXPECT_EQ(cbResp.data, nullptr);
        });

        // open
        ASSERT_TRUE(c.open("127.0.0.1", 3699));

        // ping
        EXPECT_TRUE(c.ping());

        // auth
        auto authResp = c.authenticate("root", "nebula");
        ASSERT_EQ(authResp.error_code, nebula::ErrorCode::SUCCEEDED);

        // execute
        resp = c.execute(*authResp.session_id, "SHOW SPACES");
        ASSERT_EQ(resp.error_code, nebula::ErrorCode::SUCCEEDED);
        nebula::DataSet expected({"Name"});
        EXPECT_TRUE(verifyResultWithoutOrder(*resp.data, expected));

        // explain
        resp = c.execute(*authResp.session_id, "EXPLAIN SHOW HOSTS");
        ASSERT_EQ(resp.error_code, nebula::ErrorCode::SUCCEEDED);
        EXPECT_NE(resp.plan_desc, nullptr);
        // TODO(shylock) check the plan

        // async execute
        folly::Baton<> b;
        c.asyncExecute(*authResp.session_id, "SHOW SPACES", [&b](auto &&cbResp) {
            std::cout << "DEBUG POINT: async callback";
            ASSERT_EQ(cbResp.error_code, nebula::ErrorCode::SUCCEEDED) << static_cast<int>(cbResp.error_code);
            nebula::DataSet expected({"Name"});
            EXPECT_TRUE(verifyResultWithoutOrder(*cbResp.data, expected));
            b.post();
        });
        b.wait();

        // signout
        c.signout(*authResp.session_id);

        // ping
        EXPECT_TRUE(c.ping());

        // check signout
        resp = c.execute(*authResp.session_id, "SHOW SPACES");
        ASSERT_EQ(resp.error_code, nebula::ErrorCode::E_SESSION_INVALID);

        // async execute
        folly::Baton<> b1;
        c.asyncExecute(*authResp.session_id, "SHOW SPACES", [&b1](auto &&cbResp) {
            ASSERT_EQ(cbResp.error_code, nebula::ErrorCode::E_SESSION_INVALID);
            b1.post();
        });
        b1.wait();

        // close
        c.close();

        // ping
        EXPECT_FALSE(c.ping());

        // execute
        resp = c.execute(*authResp.session_id, "SHOW SPACES");
        ASSERT_EQ(resp.error_code, nebula::ErrorCode::E_DISCONNECTED);

        // async execute
        folly::Baton<> b2;
        c.asyncExecute(*authResp.session_id, "SHOW SPACES", [&b2](auto &&cbResp) {
            ASSERT_EQ(cbResp.error_code, nebula::ErrorCode::E_DISCONNECTED);
            b2.post();
        });
        b2.wait();

        // isOpen
        EXPECT_FALSE(c.isOpen());
    }
};

TEST_F(ConnectionTest, Basic) {
    nebula::Connection c;
    LOG(INFO) << "Testing once.";
    runOnce(c);
    LOG(INFO) << "Testing reopen.";
    runOnce(c);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    nebula::init(&argc, &argv);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
