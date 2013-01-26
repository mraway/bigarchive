#include "snapshot_types.h"
#include "string.h"

void BlockMeta::Serialize(ostream& os) const
{
    os.write((char*)cksum_, CKSUM_LEN);
    marshall::Serialize(end_offset_, os);
    marshall::Serialize(handle_, os);
}

void BlockMeta::Deserialize(istream &is)
{
    is.read((char*)cksum_, CKSUM_LEN);
    marshall::Deserialize(end_offset_, is);
    marshall::Deserialize(handle_, is);
}

BlockMeta* BlockMeta::New()
{
    return new BlockMeta;
}

void BlockMeta::Copy(const Serializable& from)
{
    const BlockMeta& bm = dynamic_cast<const BlockMeta&>(from);
    handle_ = bm.handle_;
    data_ = bm.data_;
    memcpy(cksum_, bm.cksum_, CKSUM_LEN);
    end_offset_ = bm.end_offset_;
}

int64_t BlockMeta::GetSize()
{
    return sizeof(BlockMeta::end_offset_) + sizeof(BlockMeta::data_) + sizeof(BlockMeta::handle_) + CKSUM_LEN;
}


void SegmentMeta::Serialize(ostream &os) const
{
    os.write((char*)cksum_, CKSUM_LEN);
    marshall::Serialize(end_offset_, os);
    marshall::Serialize(handle_, os);
    marshall::Serialize(block_list_, os);
}

void SegmentMeta::Deserialize(istream &is)
{
    is.read((char*)cksum_, CKSUM_LEN);
    marshall::Deserialize(end_offset_, is);
    marshall::Deserialize(handle_, is);
    marshall::Deserialize(block_list_, is);
}

SegmentMeta* SegmentMeta::New()
{
    return new SegmentMeta;
}

void SegmentMeta::Copy(Serializable const &from)
{
    const SegmentMeta& sm = dynamic_cast<const SegmentMeta&>(from);
    end_offset_ = sm.end_offset_;
    handle_ = sm.handle_;
    memcpy(cksum_, sm.cksum_, CKSUM_LEN);
    block_list_ = sm.block_list_;
}

int64_t SegmentMeta::GetSize()
{
    return sizeof(SegmentMeta::end_offset_) + sizeof(SegmentMeta::handle_) + CKSUM_LEN;
}

uint32_t SegmentMeta::GetBlockSize(size_t index)
{
    if (index < block_list_.size()) {
        if (index == 0)
            return block_list_[index].end_offset_;
        return block_list_[index].end_offset_ - block_list_[index - 1].end_offset_;
    }
    return 0;
}    

uint32_t SegmentMeta::Size()
{
    return block_list_.back().end_offset_;
}

void SnapshotMeta::Serialize(ostream& os) const
{
    marshall::Serialize(vm_id_, os);
    marshall::Serialize(snapshot_id_, os);
    marshall::Serialize(size_, os);
    marshall::Serialize(segment_list_, os);
}

void SnapshotMeta::Deserialize(istream &is)
{
    marshall::Deserialize(vm_id_, is);
    marshall::Deserialize(snapshot_id_, is);
    marshall::Deserialize(size_, is);
    marshall::Deserialize(segment_list_, is);
}

SnapshotMeta* SnapshotMeta::New()
{
    return new SnapshotMeta;
}

void SnapshotMeta::Copy(const Serializable& from)
{
    const SnapshotMeta& sm = dynamic_cast<const SnapshotMeta&>(from);
    vm_id_ = sm.vm_id_;
    size_ = sm.size_;
    snapshot_id_ = sm.snapshot_id_;
    segment_list_ = sm.segment_list_;
}

int64_t SnapshotMeta::GetSize()
{
    return sizeof(SnapshotMeta::size_) + vm_id_.size() + snapshot_id_.size();
}

uint64_t SnapshotMeta::GetSegmentSize(size_t index)
{
    if (index < segment_list_.size()) {
        if (index == 0)
            return segment_list_[index].end_offset_;
        return segment_list_[index].end_offset_ - segment_list_[index - 1].end_offset_;
    }
    return 0;
}

uint64_t SnapshotMeta::Size()
{
    return segment_list_.back().end_offset_;
}

void SnapshotMeta::AddSegment(const SegmentMeta& sm)
{
    segment_list_.push_back(sm);
    size_ += segment_list_.back().Size();
    segment_list_.back().end_offset_ = size_;
}




















