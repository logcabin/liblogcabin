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

#include <signal.h>

#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Protocol/Common.h"
#include "Raft/Globals.h"
#include "Raft/RaftConsensus.h"
#include "RPC/Server.h"


namespace LogCabin {
namespace Raft {

////////// Globals //////////

Globals::Globals()
    : config()
    , eventLoop()
    , clusterUUID()
    , serverId(~0UL)
{
}

Globals::~Globals()
{
}

void
Globals::init()
{
    std::string uuid = config.read("clusterUUID", std::string(""));
    if (!uuid.empty())
        clusterUUID.set(uuid);
    serverId = config.read<uint64_t>("serverId");
    Core::Debug::processName = Core::StringUtil::format("%lu", serverId);
    /*
    if (!rpcServer) {
        rpcServer.reset(new RPC::Server(eventLoop,
                                        Protocol::Common::MAX_MESSAGE_LENGTH));

        uint32_t maxThreads = config.read<uint16_t>("maxThreads", 16);
        namespace ServiceId = Protocol::Common::ServiceId;
        rpcServer->registerService(ServiceId::RAFT_SERVICE,
                                   raftService,
                                   maxThreads);

        std::string listenAddressesStr =
            config.read<std::string>("listenAddresses");
        {
            ServerStats::Lock serverStatsLock(serverStats);
            serverStatsLock->set_server_id(serverId);
            serverStatsLock->set_addresses(listenAddressesStr);
        }
        std::vector<std::string> listenAddresses =
            Core::StringUtil::split(listenAddressesStr, ',');
        if (listenAddresses.empty()) {
            EXIT("No server addresses specified to listen on");
        }
        for (auto it = listenAddresses.begin();
             it != listenAddresses.end();
             ++it) {
            RPC::Address address(*it, Protocol::Common::DEFAULT_PORT);
            address.refresh(RPC::Address::TimePoint::max());
            std::string error = rpcServer->bind(address);
            if (!error.empty()) {
                EXIT("Could not listen on address %s: %s",
                     address.toString().c_str(),
                     error.c_str());
            }
            NOTICE("Serving on %s",
                   address.toString().c_str());
        }
        raft->serverAddresses = listenAddressesStr;
        raft->init();
    }

    if (!stateMachine) {
        stateMachine.reset(new StateMachine(raft, config, *this));
    }
    */
}

void
Globals::run()
{
    eventLoop.runForever();
}

} // namespace LogCabin::Raft
} // namespace LogCabin
