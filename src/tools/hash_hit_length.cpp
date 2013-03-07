#include "../snapshot/trace_types.h"
#include <map>
#include <iostream>
#include <fstream>
#include <string.h>
#include "../common/timer.h"

using namespace std;

struct SizeCount {
    SizeCount() { size_ = 0; ref_count_ = 0; };
    uint32_t size_;
    uint32_t ref_count_;
};

inline void save_counter(uint64_t& new_seq_counter, uint64_t& old_seq_counter, map<uint64_t, uint64_t>& old_counter_map)
{
    if (new_seq_counter == 0) {	// need to terminate old counter
        if (old_seq_counter != 0)
            old_counter_map[old_seq_counter] ++;
        old_seq_counter = 0;
    }
    ++ new_seq_counter;
}

int main( int argc, char** argv)
{
    if (argc != 3 && argc != 4) {
        cout << "Usage: " <<argv[0] << " store_file current_trace parent_trace" <<endl;
        return 0;
    }

    DOMConfigurator::configure("Log4cxxConfig.xml");

    Block blk;
    map<Checksum, SizeCount> ublks;
    map<uint64_t, uint64_t> hit_lens;
    map<uint64_t, uint64_t> miss_lens;
    map<uint64_t, uint64_t> seg_hit_lens;
    map<uint64_t, uint64_t> seg_miss_lens;
    // read unique block map, use file_id field to store count
    ifstream is(argv[1], ios_base::in | ios_base::binary);
    while (blk.FromStream(is)) {
        ublks[blk.cksum_].ref_count_ = blk.file_id_;
        ublks[blk.cksum_].size_ = blk.size_;
    }
    is.close();
    ofstream os(argv[1], ios_base::out | ios_base::binary | ios_base::trunc);

    // open trace files
    bool has_parent = false;
    ifstream cis, pis;
    cis.open(argv[2], ios_base::in | ios_base::binary);
    if (argc == 4) {
        has_parent = true;
        pis.open(argv[3], ios_base::in | ios_base::binary);
    }

    Segment cur_seg, par_seg;
    uint64_t n_misses = 0, n_hits = 0;
    uint64_t n_seg_misses = 0, n_seg_hits = 0;
    map<Checksum, SizeCount>::iterator it;

    while (cur_seg.LoadFixSize(cis)) {
        if (has_parent) {	// duplicate segment
            if (par_seg.LoadFixSize(pis) && cur_seg == par_seg) {
                save_counter(n_seg_hits, n_seg_misses, seg_miss_lens);
                continue;
            }
        }
        save_counter(n_seg_misses, n_seg_hits, seg_hit_lens);

        for (size_t i = 0; i < cur_seg.blocklist_.size(); ++i) {
            it = ublks.find(cur_seg.blocklist_[i].cksum_);
            if (it == ublks.end()) {	// miss
                ublks[cur_seg.blocklist_[i].cksum_].ref_count_ ++;
                ublks[cur_seg.blocklist_[i].cksum_].size_ = cur_seg.blocklist_[i].size_;
                save_counter(n_misses, n_hits, hit_lens);
            }
            else {	// hit
                it->second.ref_count_ ++;
                save_counter(n_hits, n_misses, miss_lens);
            }
        }
    }

    // save perfect dedup results back to disk
    for (it = ublks.begin(); it != ublks.end(); it++) {
        blk.cksum_ = it->first;
        blk.size_ = it->second.size_;
        blk.file_id_ = it->second.ref_count_;
        blk.offset_ = 0;
        blk.ToStream(os);
    }

    map<uint64_t, uint64_t>::iterator cit;
    ofstream hit_output("./hit.out", ios_base::out | ios_base::app);
    ofstream miss_output("./miss.out", ios_base::out | ios_base::app);
    ofstream seg_hit_output("./seg_hit.out", ios_base::out | ios_base::app);
    ofstream seg_miss_output("./seg_miss.out", ios_base::out | ios_base::app);
    hit_output << "snapshot: " << argv[2] << endl;
    miss_output <<  "snapshot: " << argv[2] << endl;
    seg_hit_output << "snapshot: " << argv[2] << endl;
    seg_miss_output <<  "snapshot: " << argv[2] << endl;
    if (has_parent) {
        hit_output << "parent: " << argv[3] << endl;
        miss_output <<  "parent: " << argv[3] << endl;
        seg_hit_output << "parent: " << argv[3] << endl;
        seg_miss_output <<  "parent: " << argv[3] << endl;
    }
    for (cit = hit_lens.begin(); cit != hit_lens.end(); cit ++)
        hit_output << cit->first << ", " << cit->second << endl;
    for (cit = miss_lens.begin(); cit != miss_lens.end(); cit ++)
        miss_output << cit->first << ", " << cit->second << endl;
    for (cit = seg_hit_lens.begin(); cit != seg_hit_lens.end(); cit ++)
        seg_hit_output << cit->first << ", " << cit->second << endl;
    for (cit = seg_miss_lens.begin(); cit != seg_miss_lens.end(); cit ++)
        seg_miss_output << cit->first << ", " << cit->second << endl;
    hit_output.close();
    miss_output.close();
    seg_hit_output.close();
    seg_miss_output.close();
    os.close();
    cis.close();
    if (pis.is_open())
        pis.close();
    exit(0);
}


















