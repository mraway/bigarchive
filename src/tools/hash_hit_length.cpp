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
    map<Checksum, SizeCount>::iterator it;
    while (cur_seg.LoadFixSize(cis)) {
        if (has_parent) {	// duplicate segment
            if (par_seg.LoadFixSize(pis) && cur_seg == par_seg)
                continue;
        }
        for (size_t i = 0; i < cur_seg.blocklist_.size(); ++i) {
            it = ublks.find(cur_seg.blocklist_[i].cksum_);
            if (it == ublks.end()) {	// miss
                ublks[cur_seg.blocklist_[i].cksum_].ref_count_ ++;
                ublks[cur_seg.blocklist_[i].cksum_].size_ = cur_seg.blocklist_[i].size_;
                if (n_misses == 0) {
                    if (n_hits != 0)
                        hit_lens[n_hits] ++;
                    n_hits = 0;
                }
                n_misses ++;
            }
            else {	// hit
                it->second.ref_count_ ++;
                if (n_hits == 0) {
                    if (n_misses != 0)
                        miss_lens[n_misses] ++;
                    n_misses = 0;
                }
                n_hits ++;
            }
        }
    }

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
    hit_output << "snapshot: " << argv[2] << endl;
    miss_output <<  "snapshot: " << argv[2] << endl;
    if (has_parent) {
        hit_output << "parent: " << argv[3] << endl;
        miss_output <<  "parent: " << argv[3] << endl;
    }
    for (cit = hit_lens.begin(); cit != hit_lens.end(); cit ++)
        hit_output << cit->first << ", " << cit->second << endl;
    for (cit = miss_lens.begin(); cit != miss_lens.end(); cit ++)
        miss_output << cit->first << ", " << cit->second << endl;

    hit_output.close();
    miss_output.close();
    os.close();
    cis.close();
    if (pis.is_open())
        pis.close();
    exit(0);
}


















