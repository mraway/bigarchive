#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <map>
#include <algorithm>
#include <math.h>
#include "../snapshot/trace_types.h"

using namespace std;

uint64_t raw_blocks = 0;
uint64_t raw_size = 0;
uint64_t after_l1_blocks = 0;
uint64_t after_l1_size = 0;
uint64_t after_l2_blocks = 0;
uint64_t after_l2_size = 0;
uint64_t after_l3_blocks = 0;
uint64_t after_l3_size = 0;

void usage(char* progname)
{
    cout << "Usage: " << progname << " cds_file vm_list [prefix] [suffix]" << endl;
    cout << "vm_list is a list of vm description files." << endl;
    cout << "each vm description file is a list of vm snapshot traces." << endl;
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

    std::vector<Block> cds;
    string vm_fname;
    ifstream vm_ifs, cds_ifs;
    vm_ifs.open(list_fname.c_str(), ios::in);
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

    while (vm_ifs.good()) {
        // open vm's snapshot list
        std::getline(vm_ifs, vm_fname);
        if (vm_fname.length() == 0) {
            continue;
        }
        ifstream ss_ifs(vm_fname.c_str(), ios::in);

        // open all the snapshots for that VM
        string ss_fname;
        vector<ifstream*> trace_inputs;
        while (ss_ifs.good()) {
            std::getline(ss_ifs, ss_fname);
            if (ss_fname.length() == 0) {
                continue;
            }
            string trace_fname = fprefix + ss_fname + fsuffix;
            trace_inputs.push_back(new ifstream(trace_fname.c_str(), ios::in | ios::binary));
        }
        
        // dedup each snapshot with its parent, put new blocks into gloabl index
        int num_ss = trace_inputs.size();
        Segment* segs = new Segment[num_ss];
        cout << "processing " << num_ss << " snapshots in " << vm_fname << endl;
        bool finished = false;
        while (!finished) {
            for (int j = 0; j < num_ss; j++) {
                segs[j].LoadFixSize(*trace_inputs[j]);
                raw_blocks += segs[j].blocklist_.size();
                raw_size += segs[j].size_;
                // level 1
                if (j > 0 && segs[j] == segs[j-1]) {
                    // detected at level 1
                }
                else {
                    // level 2
                    after_l1_blocks += segs[j].blocklist_.size();
                    after_l1_size += segs[j].size_;;
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
                            after_l2_blocks += 1;
                            after_l2_size += it->size_;
                            if (binary_search(cds.begin(), cds.end(), *it)) {
                                // detected at level 3
                            }
                            else {
                                after_l3_blocks += 1;
                                after_l3_size += it->size_;
                            }
                        }
                    }
                }
            }
            finished = true;
            for (int j = 0; j < num_ss; j++) {
                if (segs[j].blocklist_.size() != 0) {
                    finished = false;
                }
            }
        }
        // clean up
        delete[] segs;
        for (size_t j = 0; j < trace_inputs.size(); ++j) {
            trace_inputs[j]->close();
            delete trace_inputs[j];
        }

        cout << "raw: " << " blocks: " << raw_blocks << " size: " << raw_size << endl;
        cout << "after l1 dedup: " << " blocks: " << after_l1_blocks << " size: " << after_l1_size << endl;
        cout << "after l2 dedup: " << " blocks: " << after_l2_blocks << " size: " << after_l2_size << endl;
        cout << "after l3 dedup: " << " blocks: " << after_l3_blocks << " size: " << after_l3_size << endl;
    }
    vm_ifs.close();
    return 0;
}



















