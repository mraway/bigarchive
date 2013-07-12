#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <map>
#include <algorithm>
#include <math.h>
#include "../../snapshot/trace_types.h"
#include "../../snapshot/bloom_filter_functions.h"
#include "../../snapshot/bloom_filter.h"

using namespace std;

struct DedupCounter {
    uint64_t num_bytes_;
    uint64_t num_blocks_;

    DedupCounter() {num_bytes_ = 0; num_blocks_ = 0;}
};

void usage(char* progname)
{
    cout << "Usage: " << progname << " cds_name snapshot_list [prefix] [suffix]" << endl;
    cout << "snapshot_list is a list of vm snapshot trace files." << endl;
    cout << "prefix is used to specify the path to vm snapshot trace files." << endl;
    cout << "suffix is used to specify the trace type, such like .bv4 or .v4" << endl;
}

int main(int argc, char** argv)
{
    if (argc < 3 || argc > 5) {
        usage(argv[0]);
        return 1;
    }

    string cds_name = argv[1];
    string list_fname = argv[2];
    string fprefix, fsuffix;
    if (argc > 3) {
        fprefix = argv[3];
    }
    if (argc > 4) {
        fsuffix = argv[4];
    }

    // prepare CDS
    std::vector<Block> cds;
    ifstream cds_ifs;
    cds_ifs.open(cds_name.c_str(), ios::in | ios::binary);
    Block blk;
    while (blk.FromStream(cds_ifs)) {
        cds.push_back(blk);
    }
    cds_ifs.close();
    sort(cds.begin(), cds.end());
    cout << "CDS file " << cds_name 
         << " is loaded and sorted, it has " 
         << cds.size() << " objects" << endl;

    // open all snapshot traces
    vector<ifstream*> trace_inputs;
    ifstream ss_ifs;
    ss_ifs.open(list_fname.c_str(), ios::in);
    while (ss_ifs.good()) {
        // open all the snapshots for that VM
        string ss_fname;
        std::getline(ss_ifs, ss_fname);
        if (ss_fname.length() == 0) {
            continue;
        }
        string trace_fname = fprefix + ss_fname + fsuffix;
        trace_inputs.push_back(new ifstream(trace_fname.c_str(), ios::in | ios::binary));
    }
    ss_ifs.close();
        
    // dedup each snapshot with its parent, count the new data brought by each snapshot
    int num_ss = trace_inputs.size();
    Segment* segs = new Segment[num_ss];
    DedupCounter* new_data = new DedupCounter[num_ss];
    cout << "processing " << num_ss << " snapshots in " << list_fname << endl;
    bool finished = false;
    while (!finished) {
        for (int j = 0; j < num_ss; j++) {	// TODO: add snapshot in reverse order
            segs[j].LoadFixSize(*trace_inputs[j]);
            // level 1
            if (j > 0 && segs[j] == segs[j-1]) {
                // detected at level 1
            }
            else {
                // level 2
                // sort previous segment
                if (j != 0) {
                    sort(segs[j-1].blocklist_.begin(), segs[j-1].blocklist_.end());
                }
                for (vector<Block>::iterator it = segs[j].blocklist_.begin(); 
                     it != segs[j].blocklist_.end(); ++it) {
                    if (j != 0 
                        && binary_search(segs[j-1].blocklist_.begin(), 
                                         segs[j-1].blocklist_.end(), 
                                         *it)) {
                        // detected at level 2
                    }
                    else {
                        // level 3
                        if (binary_search(cds.begin(), cds.end(), *it)) {
                            // detected at level 3
                        }
                        else {
                            // 
                            new_data[j].num_bytes_ += it->size_;
                            new_data[j].num_blocks_ += 1;
                        }
                    }
                }
            }
        }
        // are we done?
        finished = true;
        for (int j = 0; j < num_ss; j++) {
            if (segs[j].blocklist_.size() != 0) {
                finished = false;
            }
        }
    }

    // print the new data brought by each snapshot, then reset the inputs
    for (int j = 0; j < num_ss; j++) {
        cout << "snapshot" << j << " add: "
             << new_data[j].num_blocks_ << " blocks, "
             << new_data[j].num_bytes_ << " bytes" << endl;
        trace_inputs[j]->clear();
        trace_inputs[j]->seekg(0, ios::beg);
    }

    // estimate the total number of items for bloom filter
    uint64_t begin = trace_inputs[0]->tellg();
    trace_inputs[0]->seekg(0, ios::end);
    uint64_t end = trace_inputs[0]->tellg();
    trace_inputs[0]->clear();
    trace_inputs[0]->seekg(0, ios::beg);
    uint64_t num_items = 2 * (end - begin) / 36;
    cout << "create bloom filter with these settings: " 
         << num_items << " items, "
         << BLOOM_FILTER_FP_RATE << " rate, "
         << BLOOM_FILTER_NUM_FUNCS << " functions" << endl;

    DedupCounter* del_data = new DedupCounter[num_ss];
    BloomFilter<Checksum> filter(num_items, 
                                 BLOOM_FILTER_FP_RATE, 
                                 kBloomFilterFunctions, 
                                 BLOOM_FILTER_NUM_FUNCS);
    for (int j = 0; j < num_ss - 1; j++) {
        // add snapshot into bloom filter
        trace_inputs[j]->clear();
        trace_inputs[j]->seekg(0, ios::beg);
        while (blk.FromStream(*trace_inputs[j])) {
            filter.AddElement(blk.cksum_);
        }
        // check the next snapshot as if it's going to be deleted
        while (blk.FromStream(*trace_inputs[j + 1])) {
            if (filter.Exist(blk.cksum_)) {
                // exist means keep it
            }
            else {
                // this one can be safely deleted if it's not in CDS
                if (!binary_search(cds.begin(), cds.end(), blk)) {
                    del_data[j + 1].num_blocks_ += 1;
                    del_data[j + 1].num_bytes_ += blk.size_;
                }
            }
        }
    }

    for (int j = 0; j < num_ss; j++) {
        cout << "snapshot" << j << " del: "
             << del_data[j].num_blocks_ << " blocks, "
             << del_data[j].num_bytes_ << " bytes" << endl;
    }

    // clean up
    delete[] segs;
    delete[] new_data;
    delete[] del_data;
    for (size_t j = 0; j < trace_inputs.size(); ++j) {
        trace_inputs[j]->close();
        delete trace_inputs[j];
    }
    return 0;
}



















