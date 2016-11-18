
=== Using LibLogcabin as Raft library ===

LibLogcabin has built in Raft consensus that a application can use as a library.

To incorporate Raft, the application should include:

```
#include "liblogcabin/Core/Config.h"
#include "liblogcabin/Raft/RaftConsensus.h"
```

And instantiate a RaftConsensus object, which requires a configuration object to
configure Raft specific parameters:

```
LibLogCabin::Core::Config config;
LibLogCabin::Raft::RaftConsensus raft(config);
```

For a more complete example please look at the integration test at:
src/liblogcabin/Raft/RaftIntegrationTest.cc
