#include "data_source.h"
#include "trace_types.h"
#include "string.h"

DataSource::DataSource(const string& trace_file, const string& sample_file)
{
    sample_data_ = NULL;
    try {
        // read the last record to get the snapshot size
        trace_stream_.open(trace_file.c_str(), ios_base::binary | ios_base::in);
        trace_stream_.seekg(-RECORD_SIZE, ios::end);
        Block blk;
        blk.FromStream(trace_stream_);
        snapshot_size_ = blk.offset_ + blk.size_;
        trace_stream_.clear();
        trace_stream_.seekg(0, ios::beg);

        // read some sample data from file
        ifstream sample_data_stream(sample_file.c_str(), ios_base::binary | ios_base::in);
        sample_data_stream.seekg(0, ios_base::end);
        size_t sample_data_size = sample_data_stream.tellg();
        if(sample_data_size < (SAMPLE_REGION_SIZE + RESERVED_REGION_SIZE)) {
            cout << "Error : sample does not have enough data" << endl;
            return;
        }
        sample_data_stream.seekg(0, ios_base::beg);
        sample_data_ = new char[SAMPLE_REGION_SIZE + RESERVED_REGION_SIZE];
        sample_data_stream.read(sample_data_, SAMPLE_REGION_SIZE + RESERVED_REGION_SIZE);
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
    if (sample_data_ == NULL)
        return false;
    Block blk;
    if (blk.FromStream(trace_stream_) && BlockToBlockMeta(bm, blk))
        return true;
    return false;
}

bool DataSource::BlockToBlockMeta(BlockMeta &bm, const Block& blk)
{
    if (blk.size_ == 0) {
        cout << "Error: encounter zero size block" << endl;
        return false;
    }
    if (blk.size_ > RESERVED_REGION_SIZE) {
        cout << "Error : block size is too big: " << blk.size_ << endl;
        return false;
    }
    bm.cksum_ = blk.cksum_;
    bm.end_offset_ = blk.size_;
    bm.size_ = blk.size_;
    bm.handle_ = 0;
    bm.flags_ = 0;
    bm.data_ = &sample_data_[bm.cksum_.First4Bytes() % SAMPLE_REGION_SIZE];
    return true;
}

bool DataSource::GetSegment(SegmentMeta& sm)
{
    Segment seg;
    BlockMeta bm;
    if (seg.LoadFixSize(trace_stream_)) {
        sm.segment_recipe_.clear();
        sm.segment_recipe_.reserve(seg.blocklist_.size());
        uint32_t offset = 0;
        for (size_t i = 0; i < seg.blocklist_.size(); ++i) {
            if (!BlockToBlockMeta(bm, seg.blocklist_[i]))
                return false;
            offset += seg.blocklist_[i].size_;
            bm.end_offset_ = offset;
            sm.segment_recipe_.push_back(bm);
        }
        sm.cksum_ = seg.cksum_;
        sm.end_offset_ = offset;
        sm.size_ = offset;
        sm.handle_ = 0;
        return true;
    }
    return false;
}

uint64_t DataSource::GetSnapshotSize()
{
    return snapshot_size_;
}
