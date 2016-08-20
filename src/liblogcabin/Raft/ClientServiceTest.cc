/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include "liblogcabin/Protocol/Client.pb.h"
#include "liblogcabin/Core/Buffer.h"
#include "liblogcabin/Core/ProtoBuf.h"
#include "liblogcabin/Protocol/Common.h"
#include "liblogcabin/Raft/RaftConsensus.h"
#include "liblogcabin/RPC/ClientRPC.h"
#include "liblogcabin/RPC/ClientSession.h"
#include "liblogcabin/Storage/FilesystemUtil.h"

namespace LibLogCabin {
namespace Raft {
namespace {

using LibLogCabin::Protocol::Client::OpCode;
typedef RPC::ClientRPC::Status Status;
typedef RPC::ClientRPC::TimePoint TimePoint;

class ServerClientServiceTest : public ::testing::Test {
    ServerClientServiceTest()
        : storagePath(Storage::FilesystemUtil::mkdtemp())
        , raft()
        , session()
        , thread()
    {
    }

    // Test setup is deferred for handleRPCBadOpcode, which needs to bind the
    // server port in a new thread.
    void init() {
      if (!raft) {
        Core::Config config;
        config.set("storageModule", "Memory");
        config.set("uuid", "my-fake-uuid-123");
        config.set("listenAddresses", "127.0.0.1");
        config.set("serverId", "1");
        config.set("storagePath", storagePath);
        raft.reset(new RaftConsensus(config, 1));
        RPC::Address address("127.0.0.1", LibLogCabin::Protocol::Common::DEFAULT_PORT);
        address.refresh(RPC::Address::TimePoint::max());
        session = RPC::ClientSession::makeSession(
            raft->eventLoop,
            address,
            1024 * 1024,
            TimePoint::max(),
            Core::Config());
        thread = std::thread(&Event::Loop::runForever, &raft->eventLoop);
      }
    }

    ~ServerClientServiceTest()
    {
        if (raft) {
            raft->eventLoop.exit();
            thread.join();
        }
        Storage::FilesystemUtil::remove(storagePath);
    }

    void
    call(OpCode opCode,
         const google::protobuf::Message& request,
         google::protobuf::Message& response)
    {
        RPC::ClientRPC rpc(session,
                           LibLogCabin::Protocol::Common::ServiceId::CLIENT_SERVICE,
                           1, opCode, request);
        EXPECT_EQ(Status::OK, rpc.waitForReply(&response, NULL,
                                               TimePoint::max()))
            << rpc.getErrorMessage();
    }

    // Because of the EXPECT_DEATH call below, we need to take extra care to
    // clean up the storagePath tmpdir: destructors in the child process won't
    // get a chance.
    std::string storagePath;
    std::unique_ptr<RaftConsensus> raft;
    std::shared_ptr<RPC::ClientSession> session;
    std::thread thread;
};

TEST_F(ServerClientServiceTest, handleRPCBadOpcode) {
    init();
    LibLogCabin::Protocol::Client::GetServerInfo::Request request;
    LibLogCabin::Protocol::Client::GetServerInfo::Response response;
    int bad = 255;
    OpCode unassigned = static_cast<OpCode>(bad);
    LibLogCabin::Core::Debug::setLogPolicy({ // expect warning
        {"Server/ClientService.cc", "ERROR"},
        {"", "WARNING"},
    });
    RPC::ClientRPC rpc(session,
                       LibLogCabin::Protocol::Common::ServiceId::CLIENT_SERVICE,
                       1, unassigned, request);
    EXPECT_EQ(Status::INVALID_REQUEST, rpc.waitForReply(&response, NULL,
                                                        TimePoint::max()))
        << rpc.getErrorMessage();
}

} // namespace LibLogCabin::Raft::<anonymous>
} // namespace LibLogCabin::Raft
} // namespace LibLogCabin
