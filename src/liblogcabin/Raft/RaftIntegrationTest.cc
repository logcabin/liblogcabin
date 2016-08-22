
#include <gtest/gtest.h>
#include <unistd.h>
#include <memory>
#include <sstream>

#include "liblogcabin/Core/Config.h"
#include "liblogcabin/Core/Debug.h"
#include "liblogcabin/Raft/RaftConsensus.h"

namespace LibLogCabin {
namespace Raft {

using namespace LibLogCabin::Core;

class TestServer {
public:
  TestServer(Config& config, uint64_t serverId);
  ~TestServer();
  void start();
  bool verifyCallbackData(int lastData);
  bool setConfiguration(uint64_t lastServerId);
  bool pushData(int index, const std::string& data);

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
  LibLogCabin::Protocol::Client::SetConfiguration::Request request;
  request.set_old_id(lastServerId - 1);
  for (uint64_t i = 1; i <= lastServerId; ++i) {
    LibLogCabin::Protocol::Client::Server server;
    server.set_server_id(i);
    server.set_addresses("127.0.0.1:" + std::to_string(5253 + i));
    request.add_new_servers()->CopyFrom(server);
  }

  LibLogCabin::Protocol::Client::SetConfiguration::Response response;
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

TestServer::~TestServer()
{
  raft.exit();
}

class RaftIntegrationCallbacksTest : public ::testing::Test {
public:
  RaftIntegrationCallbacksTest()
      : config1(),
        config2(),
        server1(),
        server2() {
    init();
  };

  void init() {
    config1.set("use-temporary-storage", true);
    config1.set("listenAddresses", "127.0.0.1:5254");
    server1 = std::unique_ptr<TestServer>(new TestServer(config1, 1));
    server1->start();

    config2.set("use-temporary-storage", true);
    config2.set("listenAddresses", "127.0.0.1:5255");
    server2 = std::unique_ptr<TestServer>(new TestServer(config2, 2));
    server2->start();

    ASSERT_TRUE(server1->setConfiguration(2));
  }

  Config config1;
  Config config2;
  std::unique_ptr<TestServer> server1;
  std::unique_ptr<TestServer> server2;
};

TEST_F(RaftIntegrationCallbacksTest, callbackCommitedEntries) {
  for (auto i = 0; i < 10000; i++) {
    ASSERT_TRUE(server1->pushData(i, std::to_string(i)));
  }

  ASSERT_TRUE(server1->verifyCallbackData(10000));
  ASSERT_TRUE(server2->verifyCallbackData(10000));
}

} // namespace Raft {
} // namespace LibLogCabin {
