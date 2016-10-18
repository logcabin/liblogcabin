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

#define GLOG_NO_ABBREVIATED_SEVERITIES

#include <folly/futures/Future.h>
#include <google/protobuf/message.h>
#include <memory>
#include <stdexcept>
#include <string>

#include "liblogcabin/Core/CompatAtomic.h"
#include "liblogcabin/Core/ProtoBuf.h"
#include "liblogcabin/Storage/FilesystemUtil.h"
#include "liblogcabin/Storage/SnapshotMetadata.pb.h"

#ifndef LIBLOGCABIN_STORAGE_SNAPSHOTFILE_H
#define LIBLOGCABIN_STORAGE_SNAPSHOTFILE_H

namespace LibLogCabin {
namespace Storage {

// forward declarations
class Layout;

namespace Snapshot {

/**
 * Remove any partial snapshots found on disk. This is normally called when the
 * server boots up.
 */
void discardPartialSnapshots(const Storage::Layout& storageLayout);

/**
 * Assists in reading snapshot files from the local filesystem.
 */
class Reader {
  public:
    /// Destructor.
    virtual ~Reader() {};
    /// Return the size in bytes for the file.
    virtual uint64_t getSizeBytes() = 0;
    virtual folly::Future<FilesystemUtil::FileContents*> readSnapshot() = 0;
    virtual uint8_t readVersion() = 0;
    virtual std::string readHeader(Storage::SnapshotMetadata::Header& header) = 0;
};

class DefaultReader : public Reader, Core::ProtoBuf::InputStream {
  public:
    /**
     * Constructor.
     * \param storageLayout
     *      The directories in which to find the snapshot (in a file called
     *      "snapshot" in the snapshotDir).
     * \throw std::runtime_error
     *      If the file can't be found.
     */
    explicit DefaultReader(const Storage::Layout& storageLayout);
    ~DefaultReader();

    /// Return the size in bytes for the file.
    uint64_t getSizeBytes() override;
    uint64_t getBytesRead() const override;
    std::string readHeader(Storage::SnapshotMetadata::Header& header) override;
    std::string readMessage(google::protobuf::Message& message) override;
    folly::Future<FilesystemUtil::FileContents*> readSnapshot() override;
    uint8_t readVersion() override;
    uint64_t readRaw(void* data, uint64_t length) override;

  private:
    /// Wraps the raw file descriptor; in charge of closing it when done.
    Storage::FilesystemUtil::File file;
    /// Maps the file into memory for reading.
    std::unique_ptr<Storage::FilesystemUtil::FileContents> contents;
    /// The number of bytes read from the file.
    uint64_t bytesRead;
    /// Directory that's storing the snapshot.
    Storage::FilesystemUtil::File snapshotDir;
};

class Writer : public Core::ProtoBuf::OutputStream {
  public:
    virtual ~Writer() {}

    virtual void discard() = 0;

    virtual uint64_t save() = 0;
    // See Core::ProtoBuf::OutputStream.
    virtual uint64_t getBytesWritten() const = 0;
    // See Core::ProtoBuf::OutputStream.
    virtual void writeMessage(const google::protobuf::Message& message) = 0;
    // See Core::ProtoBuf::OutputStream.
    virtual void writeRaw(const void* data, uint64_t length) = 0;
};

/**
 * Assists in writing snapshot files to the local filesystem.
 */
class DefaultWriter : public Writer {
  public:
    /**
     * Allocates an object that is shared across processes. Uses a shared,
     * anonymous mmap region internally.
     */
    template<typename T>
    struct SharedMMap {
        SharedMMap();
        ~SharedMMap();
        T* value; // pointer does not change after construction
        // SharedMMap is not copyable
        SharedMMap(const SharedMMap& other) = delete;
        SharedMMap& operator=(const SharedMMap& other) = delete;
    };

    /**
     * Constructor.
     * \param storageLayout
     *      The directories in which to create the snapshot (in a file called
     *      "snapshot" in the snapshotDir).
     * TODO(ongaro): what if it can't be written?
     */
    explicit DefaultWriter(const Storage::Layout& storageLayout);
    /**
     * Destructor.
     * If the file hasn't been explicitly saved or discarded, prints a warning
     * and discards the file.
     */
    ~DefaultWriter();
    /**
     * Throw away the file.
     * If you call this after the file has been closed, it will PANIC.
     */
    void discard();
    /**
     * Seek to the end of the file, in case another process has written to it.
     * Subsequent calls to getBytesWritten() will include data written by other
     * processes.
     */
    void seekToEnd();
    /**
     * Flush changes all the way down to the disk and close the file.
     * If you call this after the file has been closed, it will PANIC.
     * \return
     *      Size in bytes of the file
     */
    uint64_t save();
    // See Core::ProtoBuf::OutputStream.
    uint64_t getBytesWritten() const;
    // See Core::ProtoBuf::OutputStream.
    void writeMessage(const google::protobuf::Message& message);
    // See Core::ProtoBuf::OutputStream.
    void writeRaw(const void* data, uint64_t length);

  private:
    /// A handle to the directory containing the snapshot. Used for renameat on
    /// close.
    Storage::FilesystemUtil::File parentDir;
    /// The temporary name of 'file' before it is closed.
    std::string stagingName;
    /// Wraps the raw file descriptor; in charge of closing it when done.
    Storage::FilesystemUtil::File file;
    /// The number of bytes accumulated in the file so far.
    uint64_t bytesWritten;
  public:
    /**
     * This value is incremented every time bytes are written to the Writer
     * from any process holding this Writer. Used by Server/StateMachine to
     * implement a watchdog that checks progress of a snapshotting process.
     */
    SharedMMap<std::atomic<uint64_t>> sharedBytesWritten;
};

} // namespace LibLogCabin::Storage::Snapshot
} // namespace LibLogCabin::Storage
} // namespace LibLogCabin

#endif /* LIBLOGCABIN_STORAGE_SNAPSHOTFILE_H */
