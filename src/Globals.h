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

#include <memory>

#include "Client/SessionManager.h"
#include "Core/Config.h"
#include "Core/Mutex.h"
#include "Event/Loop.h"
#include "Event/Signal.h"

#ifndef LOGCABIN_RAFT_GLOBALS_H
#define LOGCABIN_RAFT_GLOBALS_H

namespace LogCabin {

namespace Raft {
class RaftConsensus;

/**
 * Holds the LogCabin daemon's top-level objects.
 * The purpose of main() is to create and run a Globals object.
 * Other classes may refer to this object if they need access to other
 * top-level objects.
 */
class Globals {
  public:
    /// Constructor.
    Globals();

    /// Destructor.
    ~Globals();

    /**
     * Finish initializing this object.
     * This should be called after #config has been filled in.
     */
    void init();

    /**
     * Run the event loop until SIGINT, SIGTERM, or someone calls
     * Event::Loop::exit().
     */
    void run();

    /**
     * Global configuration options.
     */
    Core::Config config;

    /**
     * The event loop that runs the RPC system.
     */
    Event::Loop eventLoop;

  public:
    /**
     * A unique ID for the cluster that this server may connect to. This is
     * initialized to a value from the config file. If it's not set then, it
     * may be set later as a result of learning a UUID from some other server.
     */
    Client::SessionManager::ClusterUUID clusterUUID;

    /**
     * Unique ID for this server. Set from config file.
     */
    uint64_t serverId;

    // Globals is non-copyable.
    Globals(const Globals&) = delete;
    Globals& operator=(const Globals&) = delete;

}; // class Globals

} // namespace Raft

} // namespace LogCabin

#endif /* LOGCABIN_RAFT_GLOBALS_H */
