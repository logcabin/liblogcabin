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

#include "liblogcabin/Protocol/Client.pb.h"
#include "liblogcabin/Protocol/Raft.pb.h"
#include "liblogcabin/Core/Debug.h"
#include "liblogcabin/Core/ProtoBuf.h"
#include "liblogcabin/RPC/ClientRPC.h"
#include "liblogcabin/RPC/ClientSession.h"
#include "liblogcabin/RPC/Service.h"
#include "liblogcabin/Protocol/Common.h"

#ifndef LIBLOGCABIN_SERVER_CLIENTSERVICE_H
#define LIBLOGCABIN_SERVER_CLIENTSERVICE_H

namespace LibLogCabin {
namespace Raft {

// forward declaration
class RaftConsensus;
class Replication;

/**
 * This is LogCabin's application-facing RPC service. As some of these RPCs may
 * be long-running, this is intended to run under a RPC::ThreadDispatchService.
 */
class ClientService : public RPC::Service {
  public:
    /// Constructor.
    explicit ClientService(RaftConsensus& raft);

    /// Destructor.
    ~ClientService();

    void handleRPC(RPC::ServerRPC rpc);
    std::string getName() const;

  private:
    RaftConsensus& raft;

    ////////// RPC handlers //////////

    void getServerInfo(RPC::ServerRPC rpc);
    void getConfiguration(RPC::ServerRPC rpc);
    void setConfiguration(RPC::ServerRPC rpc);
    void verifyRecipient(RPC::ServerRPC rpc);

    // ClientService is non-copyable.
    ClientService(const ClientService&) = delete;
    ClientService& operator=(const ClientService&) = delete;
}; // class ClientService

} // namespace LibLogCabin::Raft
} // namespace LibLogCabin

#endif /* LIBLOGCABIN_RAFT_CLIENTSERVICE_H */
