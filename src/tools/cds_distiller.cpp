#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <map>
#include <algorithm>
#include "../snapshot/trace_types.h"

using namespace std;

struct CdsCount {
    uint64_t total_size;
    uint64_t num_blocks;

    CdsCount() {total_size = 0; num_blocks = 0;}
};

void usage(char* progname)
{
    cout << "Usage: " << progname << " cds_name vm_list [prefix] [suffix]" << endl;
    cout << "vm_list is a list of vm description files." << endl;
    cout << "each vm description file is a list of vm snapshot traces." << endl;
    cout << "prefix is used to specify the path to vm snapshot trace files." << endl;
    cout << "suffix is used to specify the trace type, such like .bv4 or .v4" << endl;
}

void analysis_cds(map<Block, uint32_t>& global_index, uint64_t total_size)
{
    // aggregate by count
    map<Block, uint32_t>::iterator it;
    map<uint32_t, CdsCount> cds_counter;
    map<uint32_t, CdsCount>::reverse_iterator rit;
    for (it = global_index.begin(); it != global_index.end(); it++) {
        cds_counter[it->second].num_blocks += 1;
        cds_counter[it->second].total_size += it->first.size_;
    }

    // calculate the dedup size
    uint64_t dedup_size = 0;
    uint64_t dedup_blocks = 0;
    for (rit = cds_counter.rbegin(); rit != cds_counter.rend(); rit++) {
        dedup_blocks += rit->second.num_blocks;
        dedup_size += rit->second.total_size;
        //cout << rit->first << " " << rit->second.num_blocks << " " << rit->second.total_size << endl;
    }

    // calculate cds coverage from 1% to 10%
    uint64_t cds_size_threshold[10];
    uint64_t cds_freq_threshold[10];
    for (int i = 0; i < 10; i++) {
        cds_size_threshold[i] = (i + 1) * dedup_size / 100;
        //cout << cds_size_threshold[i] << endl;
    }

    // get the freq threshold 
    uint64_t cds_size = 0;
    uint64_t cds_coverage = 0;
    int i = 0;
    for (rit = cds_counter.rbegin(); rit != cds_counter.rend(); rit++) {
        cds_size += rit->second.total_size;
        cds_coverage += rit->second.total_size * rit->first;
        while (cds_size > cds_size_threshold[i]) {
            cds_freq_threshold[i] = rit->first;
            cout << "  threshold: " << (i + 1) << "% " << "freq: " << cds_freq_threshold[i]
                 << " CDS size: " << (double)100 * cds_size / dedup_size << "% "
                 << cds_size 
                 << " covered raw data: " << (double)100 * cds_coverage / total_size << "% " 
                 << cds_coverage << endl;
            if (++i >= 10) {
                break;
            }
        }
        if (i >= 10) {
            break;
        }
    }
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

    // build the global index with counter
    uint64_t raw_blocks = 0;
    uint64_t raw_size = 0;
    uint64_t total_blocks = 0;
    uint64_t total_size = 0;
    map<Block, uint32_t> global_index;
    map<Block, uint32_t>::iterator it;
    string vm_fname;
    ifstream vm_ifs;
    vm_ifs.open(list_fname.c_str(), ios::in);
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
                sort(segs[j].blocklist_.begin(), segs[j].blocklist_.end());
                raw_blocks += segs[j].blocklist_.size();
                raw_size += segs[j].size_;
                // level 1
                if (j > 0 && segs[j] == segs[j-1]) {
                    continue;
                }
                // level 2
                for (vector<Block>::iterator it = segs[j].blocklist_.begin(); 
                     it != segs[j].blocklist_.end(); ++it) {
                    if (j == 0) {
                        global_index[*it] += 1;
                        total_size += it->size_;
                        total_blocks += 1;
                    }
                    else {
                        if (!binary_search(segs[j-1].blocklist_.begin(), segs[j-1].blocklist_.end(), *it)) {
                            global_index[*it] += 1;
                            total_size += it->size_;
                            total_blocks += 1;
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
        for (int j = 0; j < trace_inputs.size(); ++j) {
            trace_inputs[j]->close();
            delete trace_inputs[j];
        }
        analysis_cds(global_index, total_size);
    }
    vm_ifs.close();

    // aggregate by count
    map<uint32_t, CdsCount> cds_counter;
    map<uint32_t, CdsCount>::reverse_iterator rit;
    for (it = global_index.begin(); it != global_index.end(); it++) {
        cds_counter[it->second].num_blocks += 1;
        cds_counter[it->second].total_size += it->first.size_;
    }

    // calculate the dedup size
    uint64_t dedup_size = 0;
    uint64_t dedup_blocks = 0;
    for (rit = cds_counter.rbegin(); rit != cds_counter.rend(); rit++) {
        dedup_blocks += rit->second.num_blocks;
        dedup_size += rit->second.total_size;
        //cout << rit->first << " " << rit->second.num_blocks << " " << rit->second.total_size << endl;
    }
    cout << "raw:" << endl;
    cout << "blocks: " << raw_blocks << endl;
    cout << "size: " << raw_size / float(1024*1024*1024) << " GB" << endl;
    cout << "after localized dedup: " << endl;
    cout << "blocks: " << total_blocks << endl;
    cout << "size: " << total_size / float(1024*1024*1024) << " GB" << endl;
    cout << "after complete dedup:" << endl;
    cout << "blocks: " << dedup_blocks << endl;
    cout << "size: " << dedup_size / float(1024*1024*1024) << " GB" << endl;

    // calculate cds coverage from 1% to 10%
    uint64_t cds_size_threshold[10];
    uint64_t cds_freq_threshold[10];
    for (int i = 0; i < 10; i++) {
        cds_size_threshold[i] = (i + 1) * dedup_size / 100;
        //cout << cds_size_threshold[i] << endl;
    }

    // get the freq threshold 
    uint64_t cds_size = 0;
    uint64_t cds_coverage = 0;
    int i = 0;
    for (rit = cds_counter.rbegin(); rit != cds_counter.rend(); rit++) {
        cds_size += rit->second.total_size;
        cds_coverage += rit->second.total_size * rit->first;
        while (cds_size > cds_size_threshold[i]) {
            cds_freq_threshold[i] = rit->first;
            cout << "threshold: " << (i + 1) << "% " << "freq: " << cds_freq_threshold[i]
                 << " CDS size: " << (double)100 * cds_size / dedup_size << "% "
                 << cds_size 
                 << " covered raw data: " << (double)100 * cds_coverage / total_size << "% " 
                 << cds_coverage << endl;
            if (++i >= 10) {
                break;
            }
        }
        if (i >= 10) {
            break;
        }
    }

    // scan the global index to generate CDS index according to threshold
    // we will only generate CDS from 1% to 5%
    ofstream cds_output[5];
    for (i = 0; i < 5; i++) {
        stringstream ss;
        ss << cds_name << "." << setfill('0') << setw(3) << (i+1);
        cds_output[i].open(ss.str().c_str(), ios::out | ios::binary | ios::trunc);
    }
    for (it = global_index.begin(); it != global_index.end(); ++ it) {
        for (i = 0; i < 5; ++i) {
            if (it->second >= cds_freq_threshold[i]) {
                it->first.ToStream(cds_output[i]);
            }
        }
    }
    for (i = 0; i < 5; ++i) {
        cds_output[i].close();
    }

    return 0;
}



















