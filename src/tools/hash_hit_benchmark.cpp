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
    LoggerPtr logger = Logger::getLogger("Benchamrk");
    Block blk;
    map<Checksum, SizeCount> ublks;

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
    uint64_t num_queries = 0;
    map<Checksum, SizeCount>::iterator it;
    while (cur_seg.LoadFixSize(cis)) {
        if (has_parent) {	// duplicate segment
            if (par_seg.LoadFixSize(pis) && cur_seg == par_seg) {
                continue;
            }
        }
        TIMER_START();
        num_queries += cur_seg.blocklist_.size();
        for (size_t i = 0; i < cur_seg.blocklist_.size(); ++i) {
            it = ublks.find(cur_seg.blocklist_[i].cksum_);
            if (it == ublks.end()) {	// miss
                ublks[cur_seg.blocklist_[i].cksum_].ref_count_ ++;
                ublks[cur_seg.blocklist_[i].cksum_].size_ = cur_seg.blocklist_[i].size_;
            }
            else {	// hit
                ++ it->second.ref_count_;
            }
        }
        TIMER_STOP();
    }

    TIMER_PRINT();
    LOG4CXX_INFO(logger, "Num of items: " << ublks.size() << " , Num of queries: " << num_queries);
    // save perfect dedup results back to disk
    for (it = ublks.begin(); it != ublks.end(); it++) {
        blk.cksum_ = it->first;
        blk.size_ = it->second.size_;
        blk.file_id_ = it->second.ref_count_;
        blk.offset_ = 0;
        blk.ToStream(os);
    }

    os.close();
    cis.close();
    if (pis.is_open())
        pis.close();
    exit(0);
}


















