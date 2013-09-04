#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <map>
#include <algorithm>
#include <math.h>
#include <cstring>
#include "../snapshot/trace_types.h"

using namespace std;

    uint64_t raw_blocks = 0;
    uint64_t raw_size = 0;
    uint64_t total_blocks = 0;
    uint64_t total_size = 0;
    uint64_t dedup_size = 0;
    uint64_t dedup_blocks = 0;

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

void analysis_cds(map<Block, uint32_t>& global_index, const string& cds_name, bool write_out = false)
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
    dedup_size = 0;
    dedup_blocks = 0;
    for (rit = cds_counter.rbegin(); rit != cds_counter.rend(); rit++) {
        dedup_blocks += rit->second.num_blocks;
        dedup_size += rit->second.total_size;
        //cout << rit->first << " " << rit->second.num_blocks << " " << rit->second.total_size << endl;
    }

    // calculate cds coverage from 1% to 10%
    uint64_t cds_size_threshold[10];	// this is how many FPs CDS shall hold
    for (int i = 0; i < 10; i++) {
        cds_size_threshold[i] = (i + 1) * dedup_blocks / 100;
        //cout << cds_size_threshold[i] << endl;
    }

    uint64_t cds_freq_threshold[10];		// this is the frequency threshold
    uint64_t num_to_add[10];
    for (int i = 0; i < 4; i++) {
        uint64_t cds_size = 0;
        uint64_t cds_data_coverage = 0;
        for (rit = cds_counter.rbegin(); rit != cds_counter.rend(); rit++) {
            if ((cds_size + rit->second.num_blocks) >= cds_size_threshold[i]) {
                num_to_add[i] = cds_size_threshold[i] - cds_size;
                cds_data_coverage += rit->first * num_to_add[i] * (rit->second.total_size / rit->second.num_blocks);
                cds_size = cds_size_threshold[i];
                cds_freq_threshold[i] = rit->first;
                break;
            }
            else {
                cds_size += rit->second.num_blocks;
                cds_data_coverage += rit->first * rit->second.total_size;
            }
        }
        //if (!write_out) {
            cout << "  threshold: " << (i + 1) << "% " << "freq: " << cds_freq_threshold[i]
                 << std::fixed << std::setw(5) << std::setprecision(6) << std::setfill( '0' )
                 << " CDS index size: " << (double)100 * cds_size / dedup_blocks << "% " << cds_size 
                 << " covered raw data: " << (double)100 * cds_data_coverage / total_size << "% " << cds_data_coverage 
                 << " alpha: " << (1 - log((double)cds_data_coverage / total_size) / log((double)cds_size / dedup_blocks))
                 << endl;
        //}
    }

    // scan the global index to generate CDS index according to threshold
    // we will only generate CDS from 1% to 5%
    if (write_out) {
        ofstream cds_output[5];
        for (int i = 0; i < 4; i++) {
            stringstream ss;
            ss << cds_name << "." << setfill('0') << setw(3) << (i+1);
            cds_output[i].open(ss.str().c_str(), ios::out | ios::binary | ios::app);
        }

        for (it = global_index.begin(); it != global_index.end(); ++ it) {
            for (int i = 0; i < 4; ++i) {
                if (it->second > cds_freq_threshold[i]) {
                    it->first.ToStream(cds_output[i]);
                    continue;
                }
                if (it->second == cds_freq_threshold[i]) {
                    if (num_to_add[i] > 0) {
                        it->first.ToStream(cds_output[i]);
                        --num_to_add[i];
                    }
                }
            }
        }

        for (int i = 0; i < 4; ++i) {
            cds_output[i].close();
        }
    }
}

int main(int argc, char** argv)
{
    //if (argc < 3 || argc > 5) {
        //usage(argv[0]);
        //return 1;
    //}

    //string cds_name = argv[1];
    //string list_fname = argv[2];
    string cds_name = "";
    string list_fname = "";
    string fprefix = "";
    string fsuffix = "";
    //if (argc > 3) {
    //    fprefix = argv[3];
    //}
    //if (argc > 4) {
    //    fsuffix = argv[4];
    //}
    int argi = 1;
    int cds_start = -1;
    int cds_every = 5;
    while (argi < argc && argv[argi][0] == '-') {
        if (strcmp(argv[argi],"--cdsname") == 0) {
            argi++;
            cds_name = argv[argi++];
        } else if (strcmp(argv[argi],"--vmlistfile") == 0) {
            argi++;
            list_fname = argv[argi++];
        } else if (strcmp(argv[argi],"--prefix") == 0) {
            argi++;
            fprefix = argv[argi++];
        } else if (strcmp(argv[argi],"--suffix") == 0) {
            argi++;
            fsuffix = argv[argi++];
        } else if (strcmp(argv[argi],"--cdsstart") == 0) {
            argi++;
            cds_start = atoi(argv[argi++]);
        } else if (strcmp(argv[argi],"--cdsevery") == 0) {
            argi++;
            cds_every = atoi(argv[argi++]);
        } else if (strcmp(argv[argi],"-?") == 0) {
            usage(argv[0]);
            exit(0);
        } else if (strcmp(argv[argi],"--") == 0) {
            argi++;
            break;
        } else {
            cout << "Unknown option: " << argv[argi];
            usage(argv[0]);
            exit(1);
        }
    }
    if (strcmp(list_fname.c_str(),"") == 0 || strcmp(cds_name.c_str(),"") == 0) {
        cout << "must specify cds name and vmlist filename" << endl;
        usage(argv[0]);
        exit(1);
    }
    bool localized_dedup = true;

    //clear the cds files
    if (cds_start < 0) {
        for (int i = 0; i < 4; i++) {
            stringstream ss;
            ss << cds_name << "." << setfill('0') << setw(3) << (i+1);
            ofstream cds_output;
            cds_output.open(ss.str().c_str(), ios::out | ios::binary | ios::trunc);
            cds_output.close();
        }
    }
    unsigned int partition_count = 8;
    unsigned int partition_index;
    int vm_index = 0;

    //loop through each partition
    for(partition_index = 0; partition_index < partition_count; partition_index++) {
        cout << "Starting Partition " << (partition_index+1) << "/" << partition_count << endl;
    raw_blocks = 0;
    raw_size = 0;
    total_blocks = 0;
    total_size = 0;
    dedup_size = 0;
    dedup_blocks = 0;
    vm_index = 0;

        // build the global index with counter
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
            
            // dedup each snapshot with its parent, put new blocks into global index
            int num_ss = trace_inputs.size();
            Segment* segs = new Segment[num_ss];
            cout << "processing " << num_ss << " snapshots in " << vm_fname << endl;
            bool finished = false;
            while (!finished) {
                for (int j = 0; j < num_ss; j++) {
                    segs[j].LoadFixSize(*trace_inputs[j]);
                    sort(segs[j].blocklist_.begin(), segs[j].blocklist_.end());
                    //these two now handled within the partitions
                    //raw_blocks += segs[j].blocklist_.size();
                    //raw_size += segs[j].size_;
                    // level 1
                    if (j > 0 && localized_dedup && segs[j] == segs[j-1]) {
                        continue;
                    }
                    // level 2
                    for (vector<Block>::iterator it = segs[j].blocklist_.begin(); 
                         it != segs[j].blocklist_.end(); ++it) {
                        //only deal with a block it it is in the current partition
                        if (((*it).cksum_.Middle4Bytes()) % partition_count == partition_index) {
                            raw_blocks++;
                            raw_size += (*it).size_;
                            if (j == 0 || !localized_dedup) {
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
            for (unsigned int j = 0; j < trace_inputs.size(); ++j) {
                trace_inputs[j]->close();
                delete trace_inputs[j];
            }
            vm_index++;
            if (cds_start >= 0 && vm_index >= cds_start &&
                    ((vm_index-cds_start) % cds_every) == 0) {
                //clear the cds before writing the first paritition
                if (partition_index == 0) {
                    for (int i = 0; i < 4; i++) {
                        stringstream ss;
                        ss << cds_name << "_" << vm_index << "." << setfill('0') << setw(3) << (i+1);
                        ofstream cds_output;
                        cds_output.open(ss.str().c_str(), ios::out | ios::binary | ios::trunc);
                        cds_output.close();
                    }
                }
                stringstream this_cds;
                this_cds << cds_name << "_" << vm_index;
                analysis_cds(global_index, this_cds.str(), true);
                cout << "Current analysis for this partition: " << endl;
                cout << "raw:" << endl;
                cout << "  blocks: " << raw_blocks << endl;
                cout << "  size: " << raw_size / float(1024*1024*1024) << " GB" << endl;
                cout << "after localized dedup: " << endl;
                cout << "  blocks: " << total_blocks << endl;
                cout << "  size: " << total_size / float(1024*1024*1024) << " GB" << endl;
                cout << "after complete dedup:" << endl;
                cout << "  blocks: " << dedup_blocks << endl;
                cout << "  size: " << dedup_size / float(1024*1024*1024) << " GB" << endl;
            }
        }
        vm_ifs.close();
        if (cds_start < 0) {
            analysis_cds(global_index, cds_name, true);
            cout << "End-of-Partition Analysis: " << endl;
            cout << "raw:" << endl;
            cout << "  blocks: " << raw_blocks << endl;
            cout << "  size: " << raw_size / float(1024*1024*1024) << " GB" << endl;
            cout << "after localized dedup: " << endl;
            cout << "  blocks: " << total_blocks << endl;
            cout << "  size: " << total_size / float(1024*1024*1024) << " GB" << endl;
            cout << "after complete dedup:" << endl;
            cout << "  blocks: " << dedup_blocks << endl;
            cout << "  size: " << dedup_size / float(1024*1024*1024) << " GB" << endl;
        }
    }

    cout << "Final Analysis: " << endl;
    cout << "raw:" << endl;
    cout << "  blocks: " << raw_blocks << endl;
    cout << "  size: " << raw_size / float(1024*1024*1024) << " GB" << endl;
    cout << "after localized dedup: " << endl;
    cout << "  blocks: " << total_blocks << endl;
    cout << "  size: " << total_size / float(1024*1024*1024) << " GB" << endl;
    cout << "after complete dedup:" << endl;
    cout << "  blocks: " << dedup_blocks << endl;
    cout << "  size: " << dedup_size / float(1024*1024*1024) << " GB" << endl;

    /*
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
    */
    return 0;
}



















