/*
 * Generates fake data from the scan trace, fake data shall come from a real vm image file
 */
#ifndef _DATA_SOURCE_H_
#define _DATA_SOURCE_H_

#include <fstream>
#include <cstdlib>
#include "snapshot_types.h"

using namespace std;

#define MAX_BLOCK_SIZE (8 * 1024 * 1024)	// the last 8MB in sample data is reserved
#define SAMPLE_REGION_SIZE (128 * 1024 * 1024)	// use the first 128MB of vm image as sample data

class DataSource
{
public:
    DataSource(const string& trace_file, const string& sample_file);

    ~DataSource();
    
    bool GetBlock(BlockMeta& bm);

    /*
     * Segment is aligned with 2MB boundary
     * Should not call this api if GetBlock is already used
     */
    bool GetSegment(SegmentMeta& sm);

    uint64_t GetSnapshotSize();

private:
    bool BlockToBlockMeta(BlockMeta& bm, const Block& blk);

private:
    char* sample_data_;
    ifstream trace_stream_;
    uint64_t snapshot_size_;
};

#endif // _DATA_SOURCE_H_



















