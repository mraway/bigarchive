/*
 * This is the class library for data source generation from trace
 */
#ifndef _DATA_SOURCE_H_
#define _DATA_SOURCE_H_

#include <fstream>
#include "snapshot_types.h"

using namespace std;

#define DATA_SOURCE_BUFFER_SIZE (4 * 1024 * 1024)

class DataSource
{
public:
    DataSource(const string& trace_file, const string& sample_file);

    ~DataSource();
    
    bool GetBlock(BlockMeta& bm);

    /*
     * Segment is aligned with 2MB boundary
     * Should not call this api if GetBlock is already called
     */
    bool GetSegment(SegmentMeta& sm);

private:
    ifstream trace_stream_;
    char* sample_data_;
    size_t sample_data_size_;
};

#endif // _DATA_SOURCE_H_



















