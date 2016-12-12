/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <gtest/gtest.h>
#include <thread>

#include "liblogcabin/Core/ProtoBuf.h"
#include "liblogcabin/Core/ProtoBufTest.pb.h"
#include "liblogcabin/Event/Loop.h"
#include "liblogcabin/Protocol/Common.h"
#include "liblogcabin/RPC/ClientRPC.h"
#include "liblogcabin/RPC/ClientSession.h"
#include "liblogcabin/RPC/OpaqueServer.h"
#include "liblogcabin/RPC/OpaqueServerRPC.h"
#include "liblogcabin/RPC/Protocol.h"
#include "liblogcabin/RPC/ServerRPC.h"

namespace LibLogCabin {
namespace RPC {
namespace {

namespace ProtocolCommon = LibLogCabin::Protocol::Common;
typedef ClientRPC::TimePoint TimePoint;

class MyServerHandler : public OpaqueServer::Handler {
    MyServerHandler()
        : lastRequest()
        , nextResponse()
        , currentRPC()
        , needsReply(false)
        , autoReply(true)
    {
    }
    void handleRPC(OpaqueServerRPC serverRPC) {
        needsReply = true;
        currentRPC = std::move(serverRPC);
        lastRequest = std::move(currentRPC.request);
        if (autoReply)
            replyOrLater();
    }
    void replyOrLater() {
        if (needsReply) {
            currentRPC.response = std::move(nextResponse);
            currentRPC.sendReply();
        } else {
            autoReply = true;
        }
    }
    Core::Buffer lastRequest;
    Core::Buffer nextResponse;
    OpaqueServerRPC currentRPC;
    bool needsReply;
    bool autoReply;
};


class RPCClientRPCTest : public ::testing::Test {
    RPCClientRPCTest()
        : eventLoop()
        , eventLoopThread(&Event::Loop::runForever, &eventLoop)
        , rpcHandler()
        , server(rpcHandler, eventLoop, ProtocolCommon::MAX_MESSAGE_LENGTH)
        , session()
        , payload()
    {
        Address address("127.0.0.1", ProtocolCommon::DEFAULT_PORT);
        address.refresh(Address::TimePoint::max());
        EXPECT_EQ("", server.bind(address));
        session = ClientSession::makeSession(eventLoop, address,
                                   ProtocolCommon::MAX_MESSAGE_LENGTH,
                                   TimePoint::max(),
                                   Core::Config());
        payload.set_field_a(3);
        payload.set_field_b(4);
    }
    void deinit() {
        eventLoop.exit();
        if (eventLoopThread.joinable())
            eventLoopThread.join();
    }
    void childDeathInit() {
        assert(!eventLoopThread.joinable());
        eventLoopThread = std::thread(&Event::Loop::runForever, &eventLoop);
    }
    ~RPCClientRPCTest()
    {
        deinit();
    }
    ServerRPC makeServerRPC()
    {
        OpaqueServerRPC opaqueServerRPC;
        opaqueServerRPC.responseTarget = &rpcHandler.nextResponse;
        ServerRPC serverRPC;
        serverRPC.opaqueRPC = std::move(opaqueServerRPC);
        serverRPC.active = true;
        return serverRPC;
    }

    Event::Loop eventLoop;
    std::thread eventLoopThread;
    MyServerHandler rpcHandler;
    OpaqueServer server;
    std::shared_ptr<ClientSession> session;
    LibLogCabin::ProtoBuf::TestMessage payload;
};

TEST_F(RPCClientRPCTest, constructor) {
    ClientRPC rpc(session, 2, 3, 4, payload);
    while (!rpc.isReady()) {
        /* spin -- can't call waitForReply because it will PANIC */;
        usleep(100);
    }
    EXPECT_LT(sizeof(Protocol::RequestHeaderVersion1),
              rpcHandler.lastRequest.getLength());
    Protocol::RequestHeaderVersion1 header =
        *static_cast<Protocol::RequestHeaderVersion1*>(
            rpcHandler.lastRequest.getData());
    header.prefix.fromBigEndian();
    EXPECT_EQ(1U, header.prefix.version);
    header.fromBigEndian();
    EXPECT_EQ(2U, header.service);
    EXPECT_EQ(3U, header.serviceSpecificErrorVersion);
    EXPECT_EQ(4U, header.opCode);
    LibLogCabin::ProtoBuf::TestMessage actual;
    EXPECT_TRUE(Core::ProtoBuf::parse(
        rpcHandler.lastRequest, actual,
        sizeof(Protocol::RequestHeaderVersion1)));
    EXPECT_EQ(payload, actual);
}

// default constructor: nothing to test
// move constructor: nothing to test
// destructor: nothing to test
// move assignment: nothing to test

TEST_F(RPCClientRPCTest, cancel) {
    ClientRPC rpc(session, 2, 3, 4, payload);
    rpc.cancel();
    EXPECT_EQ(ClientRPC::Status::RPC_CANCELED,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
    EXPECT_EQ("RPC canceled by user", rpc.getErrorMessage());
}

TEST_F(RPCClientRPCTest, waitForReply_timeout) {
    rpcHandler.autoReply = false;
    ClientRPC rpc(session, 2, 3, 4, payload);
    EXPECT_EQ(ClientRPC::Status::TIMEOUT,
              rpc.waitForReply(NULL, NULL,
                               ClientRPC::Clock::now() +
                               std::chrono::milliseconds(1)));
    makeServerRPC().reply(payload);
    // if request came in, reply now. else autoreply later.
    rpcHandler.replyOrLater();
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(NULL, NULL,
                               ClientRPC::Clock::now() +
                               std::chrono::seconds(10)));
}


// waitForReply_rpcFailed tested adequately in cancel()

TEST_F(RPCClientRPCTest, waitForReply_tooShort) {
    ClientRPC rpc(session, 2, 3, 4, payload);
    deinit();
    EXPECT_DEATH({childDeathInit();
                  rpc.waitForReply(NULL, NULL, TimePoint::max());
                 }, "too short");
}

TEST_F(RPCClientRPCTest, waitForReply_ok) {
    makeServerRPC().reply(payload);
    ClientRPC rpc(session, 2, 3, 4, payload);
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
    LibLogCabin::ProtoBuf::TestMessage actual;
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(&actual, NULL, TimePoint::max()));
    EXPECT_EQ(payload, actual);
    // should be able to call waitForReply multiple times
    LibLogCabin::ProtoBuf::TestMessage actual2;
    EXPECT_EQ(ClientRPC::Status::OK,
              rpc.waitForReply(&actual2, NULL, TimePoint::max()));
    EXPECT_EQ(payload, actual2);
}

TEST_F(RPCClientRPCTest, waitForReply_serviceSpecificError) {
    makeServerRPC().returnError(payload);
    ClientRPC rpc(session, 2, 3, 4, payload);
    EXPECT_EQ(ClientRPC::Status::SERVICE_SPECIFIC_ERROR,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
    LibLogCabin::ProtoBuf::TestMessage actual;
    EXPECT_EQ(ClientRPC::Status::SERVICE_SPECIFIC_ERROR,
              rpc.waitForReply(NULL, &actual, TimePoint::max()));
    EXPECT_EQ(payload, actual);
    // should be able to call waitForReply multiple times
    LibLogCabin::ProtoBuf::TestMessage actual2;
    EXPECT_EQ(ClientRPC::Status::SERVICE_SPECIFIC_ERROR,
              rpc.waitForReply(NULL, &actual2, TimePoint::max()));
    EXPECT_EQ(payload, actual2);
}

TEST_F(RPCClientRPCTest, waitForReply_invalidVersion) {
    makeServerRPC().reject(Protocol::Status::INVALID_VERSION);
    ClientRPC rpc(session, 2, 3, 4, payload);
    deinit();
    EXPECT_DEATH({childDeathInit();
                  rpc.waitForReply(NULL, NULL, TimePoint::max());
                 }, "client is too old");
}

TEST_F(RPCClientRPCTest, waitForReply_invalidService) {
    makeServerRPC().rejectInvalidService();
    ClientRPC rpc(session, 2, 3, 4, payload);
    EXPECT_EQ(ClientRPC::Status::INVALID_SERVICE,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
    // should be able to call waitForReply multiple times
    EXPECT_EQ(ClientRPC::Status::INVALID_SERVICE,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
}

TEST_F(RPCClientRPCTest, waitForReply_invalidRequest) {
    makeServerRPC().rejectInvalidRequest();
    ClientRPC rpc(session, 2, 3, 4, payload);
    EXPECT_EQ(ClientRPC::Status::INVALID_REQUEST,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
    // should be able to call waitForReply multiple times
    EXPECT_EQ(ClientRPC::Status::INVALID_REQUEST,
              rpc.waitForReply(NULL, NULL, TimePoint::max()));
}

TEST_F(RPCClientRPCTest, waitForReply_unknownStatus) {
    int bad = 255;
    makeServerRPC().reject(Protocol::Status(bad));
    ClientRPC rpc(session, 2, 3, 4, payload);
    deinit();
    EXPECT_DEATH({childDeathInit();
                  rpc.waitForReply(NULL, NULL, TimePoint::max());
                 }, "Unknown status");
}

} // namespace LibLogCabin::RPC::<anonymous>
} // namespace LibLogCabin::RPC
} // namespace LibLogCabin
