/* Copyright (c) 2013 Stanford University
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

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "liblogcabin/Core/Debug.h"
#include "liblogcabin/Core/StringUtil.h"
#include "liblogcabin/Core/Time.h"
#include "liblogcabin/Core/Util.h"
#include "liblogcabin/Storage/Layout.h"
#include "liblogcabin/Storage/SnapshotFile.h"

namespace LibLogCabin {
namespace Storage {
namespace Snapshot {

namespace FilesystemUtil = Storage::FilesystemUtil;
using Core::StringUtil::format;

void
discardPartialSnapshots(const Storage::Layout& layout)
{
    std::vector<std::string> files = FilesystemUtil::ls(layout.snapshotDir);
    for (auto it = files.begin(); it != files.end(); ++it) {
        const std::string& filename = *it;
        if (Core::StringUtil::startsWith(filename, "partial")) {
            NOTICE("Removing incomplete snapshot %s. This was probably being "
                   "written when the server crashed.",
                   filename.c_str());
            FilesystemUtil::removeFile(layout.snapshotDir, filename);
        }
    }
}

DefaultReader::DefaultReader(const Storage::Layout& storageLayout)
    : file()
    , contents()
    , bytesRead(0)
    , snapshotDir(FilesystemUtil::dup(storageLayout.snapshotDir))
{
    file = FilesystemUtil::tryOpenFile(snapshotDir, "snapshot", O_RDONLY);
    if (file.fd < 0) {
        throw std::runtime_error(format(
                "Snapshot file not found in %s",
                storageLayout.snapshotDir.path.c_str()));
    }
    // Assuming safe to wait on future here as default read snapshot
    // is a fast operation.
    contents.reset(readSnapshot().get());
}

DefaultReader::~DefaultReader()
{
}

folly::Future<FilesystemUtil::FileContents*> DefaultReader::readSnapshot()
{
  return folly::makeFuture<FilesystemUtil::FileContents*>(new FilesystemUtil::FileContents(
      FilesystemUtil::openFile(snapshotDir, "snapshot", O_RDONLY)));
}

uint8_t DefaultReader::readVersion()
{
  uint8_t version;
  uint64_t bytesRead = readRaw(&version, sizeof(version));
  if (bytesRead < 1) {
    // TODO(tnachen): Return Try to remove PANIC here.
    PANIC("Found completely empty snapshot file (it doesn't even "
          "have a version field)");
  }
  return version;
}

uint64_t
DefaultReader::getSizeBytes()
{
    return contents->getFileLength();
}

uint64_t
DefaultReader::getBytesRead() const
{
  return bytesRead;
}

std::string
DefaultReader::readHeader(SnapshotMetadata::Header& header)
{
  return readMessage(header);
}

std::string
DefaultReader::readMessage(google::protobuf::Message& message)
{
    uint32_t length = 0;
    uint64_t r = readRaw(&length, sizeof(length));
    if (r < sizeof(length)) {
        return format("Could only read %lu bytes of %lu-byte length field in "
                      "file %s (at offset %lu of %lu-byte file)",
                      r,
                      sizeof(length),
                      file.path.c_str(),
                      bytesRead - r,
                      getSizeBytes());
    }
    length = be32toh(length);
    if (getSizeBytes() - bytesRead < length) {
        return format("ProtoBuf is %u bytes long but there are only %lu "
                      "bytes remaining in file %s (at offset %lu)",
                      length,
                      getSizeBytes() - bytesRead,
                      file.path.c_str(),
                      bytesRead);
    }
    const Core::Buffer buf(const_cast<void*>(contents->get(bytesRead, length)),
                           length,
                           NULL);
    std::string error;
    if (!Core::ProtoBuf::parse(buf, message)) {
        error = format("Could not parse ProtoBuf at bytes %lu-%lu (inclusive) "
                       "in file %s of length %lu",
                       bytesRead,
                       bytesRead + length -1,
                       file.path.c_str(),
                       getSizeBytes());
    }
    bytesRead += length;
    if (getSizeBytes() > 1024 && // minimum to keep quiet during unit tests
        10 * bytesRead / getSizeBytes() !=
        10 * (bytesRead - length) / getSizeBytes()) {
        NOTICE("Read %lu%% of snapshot",
               100 * bytesRead / getSizeBytes());
    }
    return error;
}

uint64_t
DefaultReader::readRaw(void* data, uint64_t length)
{
    uint64_t r = contents->copyPartial(bytesRead, data, length);
    bytesRead += r;
    return r;
}

template<typename T>
DefaultWriter::SharedMMap<T>::SharedMMap()
    : value(NULL)
{
    void* addr = mmap(NULL,
                      sizeof(*value),
                      PROT_READ|PROT_WRITE,
                      MAP_SHARED|MAP_ANONYMOUS,
                      -1, 0);
    if (addr == MAP_FAILED) {
        PANIC("Could not mmap anonymous shared page: %s",
              strerror(errno));
    }
    value = new(addr) T();
}

template<typename T>
DefaultWriter::SharedMMap<T>::~SharedMMap()
{
    if (munmap(value, sizeof(*value)) != 0) {
        PANIC("Failed to munmap shared anonymous page: %s",
              strerror(errno));
    }
}

DefaultWriter::DefaultWriter(const Storage::Layout& storageLayout)
    : parentDir(FilesystemUtil::dup(storageLayout.snapshotDir))
    , stagingName()
    , file()
    , bytesWritten(0)
    , sharedBytesWritten()
{
    struct timespec now =
        Core::Time::makeTimeSpec(Core::Time::SystemClock::now());
    stagingName = format("partial.%010lu.%06lu",
                         now.tv_sec, now.tv_nsec / 1000);
    file = FilesystemUtil::openFile(parentDir, stagingName,
                                    O_WRONLY|O_CREAT|O_EXCL);
}

DefaultWriter::~DefaultWriter()
{
    if (file.fd >= 0) {
        WARNING("Discarding partial snapshot %s", file.path.c_str());
        discard();
    }
}

void
DefaultWriter::discard()
{
    if (file.fd < 0)
        PANIC("File already closed");
    FilesystemUtil::removeFile(parentDir, stagingName);
    file.close();
}

void
DefaultWriter::seekToEnd()
{
    off64_t r = lseek64(file.fd, 0, SEEK_END);
    if (r < 0)
        PANIC("lseek failed: %s", strerror(errno));
    bytesWritten = Core::Util::downCast<uint64_t>(r);
}

uint64_t
DefaultWriter::save()
{
    if (file.fd < 0)
        PANIC("File already closed");
    FilesystemUtil::fsync(file);
    uint64_t fileSize = FilesystemUtil::getSize(file);
    file.close();
    FilesystemUtil::rename(parentDir, stagingName,
                           parentDir, "snapshot");
    FilesystemUtil::fsync(parentDir);
    return fileSize;
}

uint64_t
DefaultWriter::getBytesWritten() const
{
    return bytesWritten;
}

void
DefaultWriter::writeMessage(const google::protobuf::Message& message)
{
    Core::Buffer buf;
    Core::ProtoBuf::serialize(message, buf);
    uint32_t beSize = htobe32(uint32_t(buf.getLength()));
    ssize_t r = FilesystemUtil::write(file.fd, {
                                          {&beSize, sizeof(beSize)},
                                          {buf.getData(), buf.getLength()},
                                      });
    if (r < 0) {
        PANIC("Could not write ProtoBuf into %s: %s",
              file.path.c_str(),
              strerror(errno));
    }
    bytesWritten += Core::Util::downCast<uint64_t>(r);
    *sharedBytesWritten.value += Core::Util::downCast<uint64_t>(r);
}

void
DefaultWriter::writeRaw(const void* data, uint64_t length)
{
    ssize_t r = FilesystemUtil::write(file.fd, data, length);
    if (r < 0) {
        PANIC("Could not write ProtoBuf into %s: %s",
              file.path.c_str(),
              strerror(errno));
    }
    bytesWritten += Core::Util::downCast<uint64_t>(r);
    *sharedBytesWritten.value += Core::Util::downCast<uint64_t>(r);
}

} // namespace LibLogCabin::Storage::Snapshot
} // namespace LibLogCabin::Storage
} // namespace LibLogCabin
