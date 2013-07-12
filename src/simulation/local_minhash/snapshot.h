#ifndef _SNAPSHOT_H_
#define _SNAPSHOT_H_

#include <string>
#include <vector>
#include <map>
#include "../../snapshot/trace_types.h"

using namespace std;

class Snapshot
{
public:
    Snapshot(const string& fname);

    void Clear();

    void Sort();

    vector<size_t>* FindSimilarSegments(const Checksum& cksum);

    Segment* GetSegment(size_t seg_id);

    bool SearchInSegment(size_t seg_id, const Block& blk);

private:
    vector<Segment> recipe_;
    map<Checksum, vector<size_t> > min_hash_index_;
    bool sorted;
};

#endif










