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

#include <string.h>

#include "liblogcabin/Protocol/Client.pb.h"
#include "liblogcabin/Core/Buffer.h"
#include "liblogcabin/Core/ProtoBuf.h"
#include "liblogcabin/Core/Time.h"
#include "liblogcabin/RPC/ServerRPC.h"
#include "liblogcabin/Raft/RaftConsensus.h"
#include "liblogcabin/Raft/ClientService.h"

namespace LibLogCabin {
namespace Raft {

typedef RaftConsensus::ClientResult Result;

ClientService::ClientService(RaftConsensus& raft)
    : raft(raft)
{
}

ClientService::~ClientService()
{
}

void
ClientService::handleRPC(RPC::ServerRPC rpc)
{
    using LibLogCabin::Protocol::Client::OpCode;

    // Call the appropriate RPC handler based on the request's opCode.
    switch (rpc.getOpCode()) {
        case OpCode::GET_SERVER_INFO:
            getServerInfo(std::move(rpc));
            break;
        case OpCode::GET_CONFIGURATION:
            getConfiguration(std::move(rpc));
            break;
        case OpCode::SET_CONFIGURATION:
            setConfiguration(std::move(rpc));
            break;
        case OpCode::VERIFY_RECIPIENT:
            verifyRecipient(std::move(rpc));
            break;
        default:
            WARNING("Received RPC request with unknown opcode %u: "
                    "rejecting it as invalid request",
                    rpc.getOpCode());
            rpc.rejectInvalidRequest();
    }
}

std::string
ClientService::getName() const
{
    return "ClientService";
}


/**
 * Place this at the top of each RPC handler. Afterwards, 'request' will refer
 * to the protocol buffer for the request with all required fields set.
 * 'response' will be an empty protocol buffer for you to fill in the response.
 */
#define PRELUDE(rpcClass) \
    LibLogCabin::Protocol::Client::rpcClass::Request request;     \
    LibLogCabin::Protocol::Client::rpcClass::Response response;   \
    if (!rpc.getRequest(request)) \
        return;

////////// RPC handlers //////////


void
ClientService::getServerInfo(RPC::ServerRPC rpc)
{
    PRELUDE(GetServerInfo);
    LibLogCabin::Protocol::Client::Server& info = *response.mutable_server_info();
    info.set_server_id(raft.serverId);
    info.set_addresses(raft.serverAddresses);
    rpc.reply(response);
}

void
ClientService::getConfiguration(RPC::ServerRPC rpc)
{
    PRELUDE(GetConfiguration);
    LibLogCabin::Raft::Protocol::SimpleConfiguration configuration;
    uint64_t id;
    Result result = raft.getConfiguration(configuration, id);
    if (result == Result::RETRY || result == Result::NOT_LEADER) {
        LibLogCabin::Protocol::Client::Error error;
        error.set_error_code(LibLogCabin::Protocol::Client::Error::NOT_LEADER);
        std::string leaderHint = raft.getLeaderHint();
        if (!leaderHint.empty())
            error.set_leader_hint(leaderHint);
        rpc.returnError(error);
        return;
    }
    response.set_id(id);
    for (auto it = configuration.servers().begin();
         it != configuration.servers().end();
         ++it) {
        LibLogCabin::Protocol::Client::Server* server = response.add_servers();
        server->set_server_id(it->server_id());
        server->set_addresses(it->addresses());
    }
    rpc.reply(response);
}

void
ClientService::setConfiguration(RPC::ServerRPC rpc)
{
    PRELUDE(SetConfiguration);
    Result result = raft.setConfiguration(request, response);
    if (result == Result::RETRY || result == Result::NOT_LEADER) {
        LibLogCabin::Protocol::Client::Error error;
        error.set_error_code(LibLogCabin::Protocol::Client::Error::NOT_LEADER);
        std::string leaderHint = raft.getLeaderHint();
        if (!leaderHint.empty())
            error.set_leader_hint(leaderHint);
        rpc.returnError(error);
        return;
    }
    rpc.reply(response);
}

void
ClientService::verifyRecipient(RPC::ServerRPC rpc)
{
    PRELUDE(VerifyRecipient);

    uint64_t serverId = raft.serverId;

    response.set_server_id(serverId);

    if (request.has_server_id() &&
        serverId != request.server_id()) {
        response.set_ok(false);
        response.set_error(Core::StringUtil::format(
           "Mismatched server IDs: request intended for %lu, "
           "but this server is %lu",
           request.server_id(),
           serverId));
    } else {
        response.set_ok(true);
    }
    rpc.reply(response);
}

} // namespace LibLogCabin::Raft
} // namespace LibLogCabin
