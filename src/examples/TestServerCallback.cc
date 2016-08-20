
#include <unistd.h>
#include <memory>
#include <sstream>

#include "liblogcabin/Core/Config.h"
#include "liblogcabin/Core/Debug.h"
#include "liblogcabin/Raft/RaftConsensus.h"

using namespace LibLogCabin::Raft;
using namespace LibLogCabin::Core;

namespace LibLogCabin {
namespace Examples {

class TestServer {
public:
  TestServer(Config& config, uint64_t serverId);
  ~TestServer();
  void start();
  bool verifyCallbackData(int lastData);
  bool setConfiguration(uint64_t lastServerId);
  bool pushData(int index, const std::string& data);
  bool getData(int index, std::string& output);

private:
  RaftConsensus raft;
  uint64_t serverId;
  std::vector<std::string> data;
};

TestServer::TestServer(Config& config, uint64_t serverId)
    : raft(config, serverId), serverId(serverId), data()
{
  raft.subscribeToCommittedEntries([=](std::vector<Storage::Log::Entry*> entries) {
    for (auto entry : entries) {
      if (entry->type() == Raft::Protocol::EntryType::DATA) {
        data.push_back(entry->data());
      }
    }
  });
}

void TestServer::start()
{
  raft.init();
  if (serverId == 1) {
    raft.bootstrapConfiguration();
    bool elected = false;
    while (!elected) {
      elected = raft.getLastCommitIndex().first == RaftConsensus::ClientResult::SUCCESS;
      usleep(1000);
    }
  }
}

bool TestServer::verifyCallbackData(int lastData)
{
  if (data.size() != (size_t)lastData) {
    WARNING("Server %lu only received %lu amount of data from callback, expecting %d",
            serverId, data.size(), lastData);
    //    return false;
  }

  for (auto i = 0; i < lastData; ++i) {
    int integer = atoi(data.at(i).c_str());
    if (integer != i) {
      WARNING("Expecting data at index %d to be %d, actual: %d", i, i, integer);
      return false;
    }
  }

  return true;
}

bool TestServer::setConfiguration(uint64_t lastServerId)
{
  Protocol::Client::SetConfiguration::Request request;
  request.set_old_id(lastServerId - 1);
  for (uint64_t i = 1; i <= lastServerId; ++i) {
    Protocol::Client::Server server;
    server.set_server_id(i);
    server.set_addresses("127.0.0.1:" + std::to_string(5253 + i));
    request.add_new_servers()->CopyFrom(server);
  }

  Protocol::Client::SetConfiguration::Response response;
  auto result = raft.setConfiguration(request, response);
  if (result == RaftConsensus::ClientResult::SUCCESS) {
    return true;
  }

  if (result == RaftConsensus::ClientResult::NOT_LEADER) {
    WARNING("Cannot set configuration on non-leader");
    return false;
  } else if (!response.has_ok()) {
    std::string error;
    if (response.has_configuration_changed()) {
      error = response.configuration_changed().error();
    } else {
      error = "Bad servers detected: ";
      auto badServersBegin = response.configuration_bad().bad_servers().begin();
      while (badServersBegin != response.configuration_bad().bad_servers().end()) {
        error = error + badServersBegin->addresses() + ", ";
      }
    }
    WARNING("Wasn't able to join the cluster: %s", error.c_str());
    return false;
  }

  return false;
}

bool TestServer::pushData(int index, const std::string& data)
{
  Buffer buffer;
  buffer.setData(const_cast<char*>(data.data()), sizeof(int), NULL);

  RaftConsensus::ClientResult result;
  int newIndex;
  std::tie(result, newIndex) = raft.replicate(buffer);
  if (result != RaftConsensus::ClientResult::SUCCESS) {
    WARNING("Push data failed: %d", result);
    return false;
  }

  return true;
}

bool TestServer::getData(int index, std::string& output)
{
  return false;
}

TestServer::~TestServer()
{
  raft.exit();
}

} // namespace Examples {
} // namespace LibLogCabin {

using namespace LibLogCabin::Examples;

int
main(int argc, char** argv)
{
  Config config1;
  config1.set("use-temporary-storage", true);
  config1.set("listenAddresses", "127.0.0.1:5254");
  auto server1 = std::unique_ptr<TestServer>(new TestServer(config1, 1));
  server1->start();

  Config config2;
  config2.set("use-temporary-storage", true);
  config2.set("listenAddresses", "127.0.0.1:5255");
  auto server2 = std::unique_ptr<TestServer>(new TestServer(config2, 2));
  server2->start();

  if (!server1->setConfiguration(2)) {
    return 1;
  }

  for (auto i = 0; i < 10000; i++) {
    if (!server1->pushData(i, std::to_string(i))) {
      WARNING("Unable to push data, exiting");
      return 1;
    }
  }

  std::string output;
  for (auto i = 0; i < 10000; i++) {
    if (server2->getData(i, output) && output == std::to_string(i)) {
      WARNING("Found unmatched output, expected: %d, actual: %s", i, output.c_str());
      return 1;
    }
  }

  if (!server1->verifyCallbackData(10000) || !server2->verifyCallbackData(10000)) {
    return 1;
  }

  NOTICE("Test server completed");

  return 0;
}
