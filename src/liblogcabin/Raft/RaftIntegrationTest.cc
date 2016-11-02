#include <folly/futures/Future.h>
#include <fcntl.h>
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
using namespace LibLogCabin::Storage;
using Core::StringUtil::format;

class TestSnapshotFileFactory : public Snapshot::FileFactory {
public:
  ~TestSnapshotFileFactory() {}
  class TestReader : public Snapshot::Reader {
  public:
    TestReader(const Layout& layout) :
        sizeBytes(),
        contents(),
        snapshotDir(FilesystemUtil::dup(layout.snapshotDir)) {
      auto file = FilesystemUtil::tryOpenFile(snapshotDir, "snapshot", O_RDONLY);
      if (file.fd < 0) {
        throw std::runtime_error(format(
          "Snapshot file not found in %s",
          snapshotDir.path.c_str()));
      }
      contents.reset(readSnapshot().get());
    };

    uint64_t getSizeBytes() {
      return sizeBytes;
    };

    std::string readHeader(SnapshotMetadata::Header& header) {
      uint32_t length;
      uint64_t r = contents->copyPartial(0, &length, sizeof(uint32_t));
      length = be32toh(length);
      const Core::Buffer buf(const_cast<void*>(contents->get(r, length)),
                           length,
                           NULL);
      if (!Core::ProtoBuf::parse(buf, header)) {
        return "Unable to parse protobuf";
      }

      return "";
    };

    uint8_t readVersion() {
      return 1;
    };

    folly::Future<FilesystemUtil::FileContents*> readSnapshot() {
      return folly::makeFuture(new FilesystemUtil::FileContents(
          FilesystemUtil::openFile(snapshotDir, "testSnapshot", O_RDONLY)));
    };
  private:
    uint64_t sizeBytes;
    std::unique_ptr<FilesystemUtil::FileContents> contents;
    FilesystemUtil::File snapshotDir;
  };

  class TestWriter: public Storage::Snapshot::Writer {
  public:
    TestWriter(const Storage::Layout& layout) :
        bytesWritten(),
        file(),
        snapshotDir(FilesystemUtil::dup(layout.snapshotDir)) {
      file = FilesystemUtil::openFile(snapshotDir, "testSnapshot",
                                      O_WRONLY|O_CREAT|O_EXCL);
    };

    void discard() {
      file.close();
    };

    uint64_t save() {
      FilesystemUtil::fsync(file);
      uint64_t fileSize = FilesystemUtil::getSize(file);
      return fileSize;
    };

    uint64_t getBytesWritten() const {
      return bytesWritten;
    };

    void writeMessage(const google::protobuf::Message& message) {
      Core::Buffer buf;
      Core::ProtoBuf::serialize(message, buf);
      uint32_t beSize = htobe32(uint32_t(buf.getLength()));
      ssize_t r = FilesystemUtil::write(file.fd, {
                                          {&beSize, sizeof(beSize)},
                                          {buf.getData(), buf.getLength()},
                                      });
      bytesWritten += r;
    };

    void writeRaw(const void* data, uint64_t length) {
      ssize_t r =FilesystemUtil::write(file.fd, data, length);
      bytesWritten += r;
    };
  private:
    uint64_t bytesWritten;
    FilesystemUtil::File file;
    FilesystemUtil::File snapshotDir;
  };

  Storage::Snapshot::Reader* makeReader(const Storage::Layout& storageLayout) {
    return new TestReader(storageLayout);
  };

  Storage::Snapshot::Writer* makeWriter(const Storage::Layout& storageLayout) {
    return new TestWriter(storageLayout);
  };
};

class TestServer {
public:
  TestServer(Config& config, uint64_t serverId, Snapshot::FileFactory* factory);
  ~TestServer();
  void start();
  void exit();
  bool verifyCallbackData(int lastData);
  bool setConfiguration(uint64_t lastServerId);
  bool pushData(const std::string& data);
  bool takeSnapshot();
  void readSnapshot();

private:
  RaftConsensus raft;
  uint64_t serverId;
  std::vector<std::string> data;
};

TestServer::TestServer(Config& config, uint64_t serverId, Snapshot::FileFactory* factory)
    : raft(config, serverId, factory), serverId(serverId), data()
{
  raft.subscribeToCommittedEntries([=](std::vector<Storage::Log::Entry*> entries) {
    for (auto entry : entries) {
      if (entry->type() == Raft::Protocol::EntryType::DATA) {
        data.push_back(entry->data());
      }
    }
  });
}

bool TestServer::takeSnapshot()
{
  LibLogCabin::Raft::Protocol::SimpleConfiguration configuration;
  uint64_t lastId;
  auto getResult = raft.getConfiguration(configuration, lastId);
  if (getResult != RaftConsensus::ClientResult::SUCCESS) {
    WARNING("Unable to read configuration from leader");
    return false;
  }

  auto writer = raft.beginSnapshot(lastId);
  writer->save();
  raft.snapshotDone(lastId, std::move(writer));

  return true;
}

void TestServer::readSnapshot()
{
  raft.readSnapshot();
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

void TestServer::exit()
{
  raft.exit();
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
  LibLogCabin::Raft::Protocol::SimpleConfiguration configuration;
  uint64_t lastId;
  auto getResult = raft.getConfiguration(configuration, lastId);
  if (getResult != RaftConsensus::ClientResult::SUCCESS) {
    WARNING("Unable to read configuration from leader");
    return false;
  }

  LibLogCabin::Protocol::Client::SetConfiguration::Request setRequest;
  setRequest.set_old_id(lastId);
  for (uint64_t i = 1; i <= lastServerId; ++i) {
    LibLogCabin::Protocol::Client::Server server;
    server.set_server_id(i);
    server.set_addresses("127.0.0.1:" + std::to_string(5253 + i));
    setRequest.add_new_servers()->CopyFrom(server);
  }

  LibLogCabin::Protocol::Client::SetConfiguration::Response setResponse;
  auto result = raft.setConfiguration(setRequest, setResponse);
  if (result == RaftConsensus::ClientResult::SUCCESS) {
    return true;
  }

  if (result == RaftConsensus::ClientResult::NOT_LEADER) {
    WARNING("Cannot set configuration on non-leader");
    return false;
  } else if (!setResponse.has_ok()) {
    std::string error;
    if (setResponse.has_configuration_changed()) {
      error = setResponse.configuration_changed().error();
    } else {
      error = "Bad servers detected: ";
      auto badServersBegin = setResponse.configuration_bad().bad_servers().begin();
      while (badServersBegin != setResponse.configuration_bad().bad_servers().end()) {
        error = error + badServersBegin->addresses() + ", ";
      }
    }
    WARNING("Wasn't able to join the cluster: %s", error.c_str());
    return false;
  }

  return false;
}

bool TestServer::pushData(const std::string& data)
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
  std::unique_ptr<TestServer> newServer(
      int serverId,
      Snapshot::FileFactory* factory) {
    Config config;
    config.set("use-temporary-storage", true);
    config.set("listenAddresses", "127.0.0.1:" + std::to_string(5253 + serverId));
    auto server = std::unique_ptr<TestServer>(new TestServer(config, serverId, factory));
    server->start();
    return server;
  };

  std::unique_ptr<TestServer> newServer(int serverId) {
    return newServer(serverId, nullptr);
  };
};

TEST_F(RaftIntegrationCallbacksTest, callbackCommitedEntries) {
  auto server1 = newServer(1);
  auto server2 = newServer(2);

  ASSERT_TRUE(server1->setConfiguration(2));

  for (auto i = 0; i < 10000; i++) {
    ASSERT_TRUE(server1->pushData(std::to_string(i)));
  }

  ASSERT_TRUE(server1->verifyCallbackData(10000));
  ASSERT_TRUE(server2->verifyCallbackData(10000));

  server1->exit();
  server2->exit();
}

TEST_F(RaftIntegrationCallbacksTest, callbackCommitedEntriesNewServer) {
  auto server1 = newServer(1);
  auto server2 = newServer(2);

  ASSERT_TRUE(server1->setConfiguration(2));

  for (auto i = 0; i < 10000; i++) {
    ASSERT_TRUE(server1->pushData(std::to_string(i)));
  }

  auto server3 = newServer(3);
  ASSERT_TRUE(server1->setConfiguration(3));

  EXPECT_TRUE(server1->verifyCallbackData(10000));
  EXPECT_TRUE(server2->verifyCallbackData(10000));
  EXPECT_TRUE(server3->verifyCallbackData(10000));

  server1->exit();
  server2->exit();
  server3->exit();
}

TEST_F(RaftIntegrationCallbacksTest, testSnapshotHandler) {
  auto server1 = newServer(1, new TestSnapshotFileFactory());
  auto server2 = newServer(2, new TestSnapshotFileFactory());

  ASSERT_TRUE(server1->setConfiguration(2));

  for (auto i = 0; i < 10000; i++) {
    ASSERT_TRUE(server1->pushData(std::to_string(i)));
  }

  EXPECT_TRUE(server1->verifyCallbackData(10000));
  EXPECT_TRUE(server2->verifyCallbackData(10000));

  EXPECT_TRUE(server1->takeSnapshot());
  server1->readSnapshot();

  server1->exit();
  server2->exit();
}


} // namespace Raft {
} // namespace LibLogCabin {
