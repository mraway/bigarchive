#include "snapshot.h"
#include <fstream>
#include <algorithm>

Snapshot::Snapshot(const string& fname)
{
    ifstream ifs(fname.c_str(), ios::in | ios::binary);
    Segment seg;
    sorted = false;
    while (seg.LoadFixSize(ifs)) {
        recipe_.push_back(seg);
    }
    ifs.close();
}

void Snapshot::Sort()
{
    for (size_t i = 0; i < recipe_.size(); i++) {
        //cout << "minhash: " << i << " " << recipe_[i].GetMinHash().ToString() << endl;
        min_hash_index_[recipe_[i].GetMinHash()].push_back(i);
        sort(recipe_[i].blocklist_.begin(), recipe_[i].blocklist_.end());
    }
    sorted = true;
}

void Snapshot::Clear()
{
    recipe_.clear();
    min_hash_index_.clear();
}

vector<size_t>* Snapshot::FindSimilarSegments(const Checksum& cksum)
{
    map<Checksum, vector<size_t> >::iterator it;
    it = min_hash_index_.find(cksum);
    if (it != min_hash_index_.end()) {
        return &(it->second);
    }
    else {
        return NULL;
    }
}

Segment* Snapshot::GetSegment(size_t seg_id) {
    if (seg_id < recipe_.size()) {
        return &(recipe_[seg_id]);
    }
    else {
        return NULL;
    }
}

bool Snapshot::SearchInSegment(size_t seg_id, const Block& blk) 
{
    if (!sorted) {
        Sort();
    }

    if (seg_id < recipe_.size()) {
        return binary_search(recipe_[seg_id].blocklist_.begin(), recipe_[seg_id].blocklist_.end(), blk);
    }
    else {
        return false;
    }
}

















