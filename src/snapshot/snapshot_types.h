/*
 * Define the basic types for snapshot deduplication
 */

#ifndef _SNAPSHOT_TYPES_H_
#define _SNAPSHOT_TYPES_H_

#include "serialize.h"
#include "dedup_types.h"
#include <vector>

using namespace std;

/* although append store use string as handle type,
 * in our metadata it's still better to use 8 bytes int to avoid string allocation
 */
typedef uint64_t HandleType;	

class BlockMeta : public marshall::Serializable
{
public:
    uint32_t end_offset_;	// records the offset position right after the last byte
    Checksum cksum_;
    HandleType handle_;
    char* data_;

public:
    /* override */ void Serialize(ostream& os) const;
    /* override */ void Deserialize(istream& is);
    /* override */ BlockMeta* New();
    /* override */ void Copy(const Serializable& from);
    /* override */ int64_t GetSize();
};

class SegmentMeta : public marshall::Serializable
{
public:
    vector<BlockMeta> block_list_;
    uint64_t end_offset_;
    Checksum cksum_;	// this is the sha-1 hash of the sequence of its block hashes
    HandleType handle_;

public:
    /* override */ void Serialize(ostream& os) const;
    /* override */ void Deserialize(istream& is);
    /* override */ SegmentMeta* New();
    /* override */ void Copy(const Serializable& from);
    /* override */ int64_t GetSize();
    uint32_t GetBlockSize(size_t index);
    uint32_t Size();
};

class SnapshotMeta : public marshall::Serializable
{
public:
    vector<SegmentMeta> segment_list_;
    uint64_t size_;	// overall size of snapshot in bytes
    string vm_id_;
    string snapshot_id_;

public:
    /* override */ void Serialize(ostream& os) const;
    /* override */ void Deserialize(istream& is);
    /* override */ SnapshotMeta* New();
    /* override */ void Copy(const Serializable& from);
    /* override */ int64_t GetSize();
    uint64_t GetSegmentSize(size_t index);
    uint64_t Size();
    void AddSegment(const SegmentMeta& sm);
};

#endif // _SNAPSHOT_TYPES_H_














