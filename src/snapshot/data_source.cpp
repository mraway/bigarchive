#include "data_source.h"
#include "dedup_types.h"
#include "string.h"

DataSource::DataSource(const string& trace_file, const string& sample_file)
{
    sample_data_ = NULL;
    sample_data_size_ = 0;
    try {
        trace_stream_.open(trace_file.c_str(), ios_base::binary | ios_base::in);
        ifstream sample_data_stream(sample_file.c_str(), ios_base::binary | ios_base::in);
        sample_data_stream.seekg(0, ios_base::end);
        sample_data_size_ = sample_data_stream.tellg();
        sample_data_stream.seekg(0, ios_base::beg);
        sample_data_ = new char[sample_data_size_];
        sample_data_stream.read(sample_data_, sample_data_size_);
        sample_data_stream.close();
    }
    catch (exception& e) {
        throw e;
    }
}

DataSource::~DataSource()
{
    if (trace_stream_.is_open())
        trace_stream_.close();
    if (sample_data_ != NULL)
        delete[] sample_data_;
}

bool DataSource::GetBlock(BlockMeta& bm)
{
    Block blk;
    if (blk.FromStream(trace_stream_))
    {
        if (blk.size_ > sample_data_size_)
            return false;
        memcpy(bm.cksum_, blk.cksum_, CKSUM_LEN);
        bm.end_offset_ = blk.size_;
        bm.data_ = sample_data_;
        bm.handle_ = 0;
        return true;
    }
    return false;
}

bool DataSource::GetSegment(SegmentMeta& sm)
{
    Segment seg;
    BlockMeta bm;
    if (seg.LoadFixSize(trace_stream_)) {
        sm.block_list_.clear();
        sm.block_list_.reserve(seg.blocklist_.size());
        uint32_t offset = 0;
        for (int i = 0; i < seg.blocklist_.size(); ++i) {
            if (seg.blocklist_[i].size_ > sample_data_size_)
                return false;
            memcpy(bm.cksum_, seg.blocklist_[i].cksum_, CKSUM_LEN);
            offset += seg.blocklist_[i].size_;
            bm.end_offset_ = offset;
            bm.data_ = sample_data_;
            bm.handle_ = 0;
            sm.block_list_.push_back(bm);
        }
        memcpy(sm.cksum_, seg.cksum_, CKSUM_LEN);
        sm.end_offset_ = offset;
        sm.handle_ = 0;
        return true;
    }
    return false;
}




















