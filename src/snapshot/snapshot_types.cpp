#include "snapshot_types.h"
#include "string.h"

BlockMeta::BlockMeta()
{
    handle_ = 0;
    flags_ = 0;
    end_offset_ = 0;
    size_ = 0;
    data_ = NULL;
}

void BlockMeta::Serialize(ostream& os) const
{
    cksum_.ToStream(os);
    marshall::Serialize(end_offset_, os);
    marshall::Serialize(handle_, os);
    marshall::Serialize(flags_, os);
}

void BlockMeta::Deserialize(istream &is)
{
    cksum_.FromStream(is);
    marshall::Deserialize(end_offset_, is);
    marshall::Deserialize(handle_, is);
    marshall::Deserialize(flags_, is);
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
    cksum_ = bm.cksum_;
    end_offset_ = bm.end_offset_;
    flags_ = bm.flags_;
    size_ = bm.size_;
}

int64_t BlockMeta::GetSize()
{
    return sizeof(BlockMeta::end_offset_) + sizeof(BlockMeta::data_) 
        + sizeof(BlockMeta::handle_) + sizeof(BlockMeta::flags_) + CKSUM_LEN;
}

uint32_t BlockMeta::GetBlockSize()
{
    return size_;
}

uint64_t BlockMeta::SetHandle(const string& handle)
{
    if (handle.size() != sizeof(HandleType))
        return 0;
    memcpy((char*)&handle_, handle.c_str(), sizeof(HandleType));
    return handle_;
}

string BlockMeta::GetHandle()
{
    string handle((char*)&handle_, sizeof(HandleType));
    return handle;
}

/************************** SegmentMeta **************************/

void SegmentMeta::Serialize(ostream &os) const
{
    cksum_.ToStream(os);
    marshall::Serialize(end_offset_, os);
    marshall::Serialize(handle_, os);
}

void SegmentMeta::Deserialize(istream &is)
{
    cksum_.FromStream(is);
    marshall::Deserialize(end_offset_, is);
    marshall::Deserialize(handle_, is);
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
    cksum_ = sm.cksum_;
    size_ = sm.size_;
    segment_recipe_ = sm.segment_recipe_;
}

int64_t SegmentMeta::GetSize()
{
    return sizeof(SegmentMeta::end_offset_) + sizeof(SegmentMeta::handle_)
        + sizeof(SegmentMeta::size_) + CKSUM_LEN;
}

uint32_t SegmentMeta::GetBlockSize(size_t index)
{
    if (index < segment_recipe_.size()) {
        return segment_recipe_[index].size_;
    }
    return 0;
}    

uint32_t SegmentMeta::GetSegmentSize()
{
    return size_;
}

void SegmentMeta::SerializeRecipe(ostream& os) const
{
    marshall::Serialize(segment_recipe_, os);
}

void SegmentMeta::DeserializeRecipe(istream& is)
{
    marshall::Deserialize(segment_recipe_, is);
    if (segment_recipe_.size() == 0)
        return;
    for (size_t i = 1; i < segment_recipe_.size(); ++i)
    {
        segment_recipe_[i].size_ = segment_recipe_[i].end_offset_ 
            - segment_recipe_[i-1].end_offset_;
    }
}

uint64_t SegmentMeta::SetHandle(const string& handle)
{
    if (handle.size() != sizeof(HandleType))
        return 0;
    memcpy((char*)&handle_, handle.c_str(), sizeof(HandleType));
    return handle_;
}

string SegmentMeta::GetHandle()
{
    string handle((char*)&handle_, sizeof(HandleType));
    return handle;
}

void SegmentMeta::BuildIndex()
{
    if (blkmap_.size() != 0)
        blkmap_.clear();
    for (size_t i = 0; i < segment_recipe_.size() ; ++i)
        blkmap_[segment_recipe_[i].cksum_] = &segment_recipe_[i];
}

BlockMeta* SegmentMeta::SearchBlock(const Checksum& cksum)
{
    map<Checksum, BlockMeta*>::iterator it = blkmap_.find(cksum);
    if (it == blkmap_.end())
        return NULL;
    return it->second;
}

/************************** SnapshotMeta ***************************/

void SnapshotMeta::Serialize(ostream& os) const
{
    marshall::Serialize(vm_id_, os);
    marshall::Serialize(snapshot_id_, os);
    marshall::Serialize(size_, os);
}

void SnapshotMeta::Deserialize(istream &is)
{
    marshall::Deserialize(vm_id_, is);
    marshall::Deserialize(snapshot_id_, is);
    marshall::Deserialize(size_, is);
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
    snapshot_recipe_ = sm.snapshot_recipe_;
}

int64_t SnapshotMeta::GetSize()
{
    return sizeof(SnapshotMeta::size_) + vm_id_.size() + snapshot_id_.size();
}

uint32_t SnapshotMeta::GetSegmentSize(size_t index)
{
    if (index < snapshot_recipe_.size()) {
        return snapshot_recipe_[index].GetSegmentSize();
    }
    return 0;
}

uint64_t SnapshotMeta::GetSnapshotSize()
{
    return size_;
}

void SnapshotMeta::AddSegment(const SegmentMeta& sm)
{
    snapshot_recipe_.push_back(sm);
    size_ += snapshot_recipe_.back().GetSegmentSize();
    snapshot_recipe_.back().end_offset_ = size_;
}

void SnapshotMeta::SerializeRecipe(ostream& os) const
{
    marshall::Serialize(snapshot_recipe_, os);
}

void SnapshotMeta::DeserializeRecipe(istream& is)
{
    marshall::Deserialize(snapshot_recipe_, is);
    if (snapshot_recipe_.size() == 0)
        return;
    for (size_t i = 1; i < snapshot_recipe_.size(); ++i)
    {
        snapshot_recipe_[i].size_ = snapshot_recipe_[i].end_offset_ 
            - snapshot_recipe_[i-1].end_offset_;
    }
}




















