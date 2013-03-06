/*
 * Verify that CDS is set properly in memcached
 */

#include <fstream>
#include "cds_index.h"
#include "data_source.h"
#include "trace_types.h"
#include "snapshot_types.h"
#include "../common/timer.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

void usage(char* progname)
{
    cout << "Usage: " << progname << ": cds_file" << endl;
}

int main(int argc, char** argv)
{
    if (argc != 2) {
        usage(argv[0]);
        return -1;
    }
    DOMConfigurator::configure("Log4cxxConfig.xml");
    string cds_pathname(argv[1]);
    ifstream is(cds_pathname, ios::in | ios::binary);
    CdsIndex cds;
    Block blk;
    uint64_t offset_in_cds = 0, offset_in_trace = 0;
    uint64_t num_total = 0, num_hits = 0, num_misses = 0, num_errs = 0;

    TIMER_START();
    while (blk.FromStream(is)) {
        ++ num_total;
        bool result = false;
        result = cds.Get(blk.cksum_, &offset_in_cds);
        if (!result) {
            cout << "Block not found in CDS: " << blk.ToString() 
                 << " offset: " << offset_in_trace << endl;
            ++ num_misses;
        }
        else if (offset_in_trace != offset_in_cds) {
            cout << "Block found with incorrect offset: " << blk.ToString() 
                 << " in trace: " << offset_in_trace 
                 << " in CDS: " << offset_in_cds << endl;
            ++ num_errs;
        }
        else
            ++ num_hits;
        offset_in_trace += blk.size_;
    }
    TIMER_PRINT();

    cout << num_total << " checked, "
         << num_hits << " hits, "
         << num_misses << " misses, "
         << num_errs << " errors." << endl;

    is.clear();
    is.seekg(0, ios::beg);
    Segment seg;
    Checksum* cksums = new Checksum[2000];
    uint64_t* offsets = new uint64_t[2000];
    bool* results = new bool[2000];
    uint64_t num_queries = 0;
    num_total = 0;

    TIMER_START();
    while (seg.LoadFixSize(is)) {
        num_queries = 0;
        for (size_t i = 0; i < seg.blocklist_.size(); i++) {
            results[num_queries] = false;
            cksums[num_queries++] = seg.blocklist_[i].cksum_;
        }
        cds.BatchGet(cksums, num_queries, results, offsets);
        for (size_t i = 0; i < num_queries; i++) {
            if (!results[i])
                cout << "Block not found in CDS" << endl;
        }
        num_total += seg.blocklist_.size();
    }
    TIMER_PRINT();

    cout << num_total << " checked in batch mode." << endl;
    delete[] cksums;
    delete[] results;
    delete[] offsets;
    is.close();
    exit(0);
}

















