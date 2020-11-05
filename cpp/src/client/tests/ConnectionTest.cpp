/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <nebula/client/Connection.h>
#include <nebula/client/Init.h>

// Require a nebula server could access

class ClientTest : public ::testing::Test {
protected:
    static ::testing::AssertionResult verifyResultWithoutOrder(const nebula::DataSet& resp,
                                                               const nebula::DataSet& expect) {
        nebula::DataSet respCopy = resp;
        nebula::DataSet expectCopy = expect;
        std::sort(respCopy.rows.begin(), respCopy.rows.end());
        std::sort(expectCopy.rows.begin(), expectCopy.rows.end());
        if (respCopy != expectCopy) {
            return ::testing::AssertionFailure() << "Resp is : " << resp << std::endl
                                                 << "Expect : " << expect;
        }
        return ::testing::AssertionSuccess();
    }
};

class ConnectionTest : public ClientTest {
protected:
    static void runOnce(nebula::Connection& c) {
        // ping
        EXPECT_FALSE(c.ping());

        // open
        ASSERT_TRUE(c.open("127.0.0.1", 3699));

        // ping
        EXPECT_TRUE(c.ping());

        // auth
        auto authResp = c.authenticate("root", "nebula");
        ASSERT_EQ(authResp.code, nebula::ErrorCode::SUCCEEDED);

        // execute
        auto resp = c.execute(authResp.sessionId, "SHOW SPACES");
        ASSERT_EQ(resp.code, nebula::ErrorCode::SUCCEEDED);
        nebula::DataSet expected({"Name"});
        EXPECT_TRUE(verifyResultWithoutOrder(*resp.data, expected));

        // signout
        c.signout(authResp.sessionId);

        // ping
        EXPECT_TRUE(c.ping());

        // check signout
        resp = c.execute(authResp.sessionId, "SHOW SPACES");
        ASSERT_EQ(resp.code, nebula::ErrorCode::E_SESSION_INVALID);

        // close
        c.close();

        // ping
        EXPECT_FALSE(c.ping());

        // execute
        resp = c.execute(authResp.sessionId, "SHOW SPACES");
        ASSERT_EQ(resp.code, nebula::ErrorCode::E_RPC_FAILURE);

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

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    nebula::init(&argc, &argv);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
