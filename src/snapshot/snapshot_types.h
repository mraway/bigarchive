/*
 * Define the basic types for snapshot deduplication
 */

#ifndef _SNAPSHOT_TYPES_H_
#define _SNAPSHOT_TYPES_H_

#include "../include/serialize.h"
#include "trace_types.h"
#include <vector>

using namespace std;

#define IN_CDS 0x1	// in common data store
#define IN_AS 0x2	// in VM's own append store

/* although append store use string as handle type,
 * in our metadata it's still better to use 8 bytes int to avoid string allocation
 */
typedef uint64_t HandleType;	

class BlockMeta : public marshall::Serializable
{
public:
    uint32_t end_offset_;	// records the offset position right after the last byte
    uint32_t size_;
    Checksum cksum_;
    HandleType handle_;
    uint16_t flags_;
    char* data_;

public:
	// serialize the metadata of a block
    /* override */ void Serialize(ostream& os) const;
    /* override */ void Deserialize(istream& is);

    /* override */ BlockMeta* New();
    /* override */ void Copy(const Serializable& from);
    /* override */ int64_t GetSize();
    uint32_t GetBlockSize();
};

class SegmentMeta : public marshall::Serializable
{
public:
    vector<BlockMeta> segment_recipe_;
    uint64_t end_offset_;
    uint32_t size_;
    Checksum cksum_;	// this is the sha-1 hash of the sequence of its block hashes
    HandleType handle_;

public:
	// serialize the metadata of a segment
    /* override */ void Serialize(ostream& os) const;
    /* override */ void Deserialize(istream& is);

    /* override */ SegmentMeta* New();
    /* override */ void Copy(const Serializable& from);
    /* override */ int64_t GetSize();

    uint32_t GetBlockSize(size_t index);
    uint32_t GetSegmentSize();

    // serialize the recipe of segment
    void SerializeRecipe(ostream& os) const;
    void DeserializeRecipe(istream& is);
};

class SnapshotMeta : public marshall::Serializable
{
public:
    vector<SegmentMeta> snapshot_recipe_;
    uint64_t size_;	// overall size of snapshot in bytes
    string vm_id_;
    string snapshot_id_;

public:
    // serialize the meta data of snapshot
    /* override */ void Serialize(ostream& os) const;
    /* override */ void Deserialize(istream& is);

    /* override */ SnapshotMeta* New();
    /* override */ void Copy(const Serializable& from);
    /* override */ int64_t GetSize();
    uint32_t GetSegmentSize(size_t index);
    uint64_t GetSnapshotSize();
    void AddSegment(const SegmentMeta& sm);

	// serialize the recipe of snapshot
    void SerializeRecipe(ostream& os) const;
    void DeserializeRecipe(istream& is);
};

#endif // _SNAPSHOT_TYPES_H_














