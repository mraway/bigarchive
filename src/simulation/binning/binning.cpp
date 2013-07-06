/*
 * this program split list of blocks into large segments,
 * using archor to determine the boundary of segments
 */

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <sstream>

#include "dedup_types.hpp"

using namespace std;

void usage(char *progname)
{
    pr_msg("This program group a list of segments into bins by their min-hash, then simulate the dedupe process in every bin.\n"
           "Usage: %s segment_file", progname);
}

/*
 * Load a 2MB segment from trace
 */

bool load_segment(Segment& seg, ifstream& is)
{
    Block blk;
    seg.Init();
    while (blk.Load(is)) {
        if (blk.GetSize() == 0) {	// fix the zero-sized block bug in scanner
            //pr_msg("ignore zero-sized block");
            continue;
        }
        seg.AddBlock(blk);
        if (seg.GetSize() >= 2*1024*1024)
            break;
    }
    seg.Final();
    //seg.Final();
    if (seg.GetSize() == 0 || seg.blocklist_.size() == 0) 
        return false;
    else
        return true;
}

//load a segment, and make random changes
//each block read has a <blk_threshold> chance of being randomly modified
//if consistency is desired, seed the random number generator consistently
bool load_rand_segment(Segment& seg, ifstream& is, double blk_threshold)
{
    if (blk_threshold == 0)
        return load_segment(seg,is);
    Block blk;
    seg.Init();
    double rd;
    while (blk.Load(is)) {
        if (blk.GetSize() == 0) {	// fix the zero-sized block bug in scanner
            //pr_msg("ignore zero-sized block");
            continue;
        }
        rd = (double)rand() / RAND_MAX;
        if (rd <= blk_threshold)
        {
            rd = (double)rand() / RAND_MAX;
            memcpy(&blk.cksum_[4],&rd,sizeof(rd));
        }
        seg.AddBlock(blk);
        if (seg.GetSize() >= 2*1024*1024)
            break;
    }
    seg.Final();
    //seg.Final();
    if (seg.GetSize() == 0 || seg.blocklist_.size() == 0) 
        return false;
    else
        return true;
}

int main(int argc, char **argv)
{
    Segment current_seg;
    ifstream current_input, trace_input, vmlist_input;
    bool use_trace_file = false;
    bool use_vm_list = false;
    int vmtraceindex = 0;
    int vmindex = 0;
    int trace_index = 0;
    int argindex = 1;

    map<string, Bin> binmap;
    map<Hash, int> theoreticalIndex;
    uint64_t theory_dedup_size = 0;
    Segment seg;
    int num_bins;
    uint64_t total_size = 0;
    uint64_t total_dedup_size = 0;
    uint64_t total_blocks = 0;
    uint64_t total_dedup_blocks = 0;
    uint64_t total_segments = 0;
    uint64_t theory_dedup_blocks;

    string tracefile_prefix, tracefile_suffix;
    map<string, Bin>::iterator it;

    while(argindex < argc && argv[argindex][0] == '-')
    {
        if (strcmp(argv[argindex],"--tracefile") == 0) {
            use_trace_file = true;
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            trace_input.open(argv[argindex++], std::ios_base::in);
            if (!trace_input.is_open()) {
                pr_msg("unable to open %s", argv[argindex-1]);
                exit(1);
            }
	} else if (strcmp(argv[argindex],"--traceprefix") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            tracefile_prefix = argv[argindex++];
        } else if (strcmp(argv[argindex],"--tracesuffix") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            tracefile_suffix = argv[argindex++];
        } else if (strcmp(argv[argindex],"--vmlistfile") == 0) {
            use_vm_list = true;
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            vmlist_input.open(argv[argindex++], std::ios_base::in);
            if (!vmlist_input.is_open()) {
                pr_msg("unable to open %s", argv[argindex-1]);
                exit(1);
            }
        } else if (strcmp(argv[argindex],"--") == 0) {
            argindex++;
            break;
        } else {
            cout << "Invalid argument. Use -- before using filename args beginning with '-'" << endl;
            usage(argv[0]);
            return 0;
        }
    }

    while((!use_trace_file && argindex < argc) || (use_trace_file && !use_vm_list && trace_input.good()) || (use_vm_list && vmlist_input.good())) {
        string line;
        const char *cur_vm_path;
        vmtraceindex = 0;
        if (use_vm_list) {
            getline(vmlist_input,line);
            cur_vm_path = line.c_str();
            if (strlen(cur_vm_path) < 1 || cur_vm_path[0] == '\n') {
                continue;
            }
            if (trace_input.is_open()) {
                trace_input.close();
            }
            trace_input.open(cur_vm_path, std::ios_base::in);
            if (!trace_input.is_open()) {
                pr_msg("unable to open %s", cur_vm_path);
                exit(1);
            } else {
                cout << "VM file (" << vmindex << "): " << cur_vm_path << endl;
                use_trace_file = true;
            }
        }
        while((!use_trace_file && argindex < argc) || (use_trace_file && trace_input.good())) {
            const char *cur_trace_path;
            string cur_tracefile_name;
            stringstream tracepath_stream;
            Hash curHash;
            std::vector<Block>::iterator iter;
            if (use_trace_file) {
                getline(trace_input,cur_tracefile_name);
            } else {
                cur_tracefile_name = argv[argindex++];
            }
            tracepath_stream << tracefile_prefix << cur_tracefile_name << tracefile_suffix;
            cur_trace_path = tracepath_stream.str().c_str();

            if (strlen(cur_trace_path) > 0 && cur_trace_path[0] != '\n') {
                if (current_input.is_open()) {
                    current_input.close();
                }
                printf("snapshot trace file %d.%d: %s\n", vmindex, vmtraceindex,cur_trace_path);
                current_input.open(cur_trace_path, std::ios_base::in | std::ios_base::binary);
                if (!current_input.is_open()) {
                    pr_msg("unable to open %s", cur_trace_path);
                    exit(1);
                }
                //theoretical(indexBlocks, current_input, blk_threshold, seed);

                while (load_segment(seg,current_input)) {
                    binmap[seg.GetMinHashString()].AddSegment(seg);
                    for(iter = seg.blocklist_.begin();iter != seg.blocklist_.end(); ++iter) {
                        curHash.setHash(*iter);
                        theoreticalIndex[curHash]++;
                        if (theoreticalIndex[curHash] == 1) {
                            theory_dedup_size += (uint64_t)((*iter).size_);
                        }
                        total_size += (*iter).size_;
                        total_blocks++;
                    }
                    total_segments++;
                }
                if (!use_vm_list) {
                    //vm_blocks.clear();
                }

                //output.close();
                //parent_input.close();
                current_input.close();
            }
    num_bins = binmap.size();
    total_dedup_size = 0;
    total_dedup_blocks = 0;
    for (it=binmap.begin(); it != binmap.end(); it ++) {
        //total_size += (*it).second.getTotalSize();
        total_dedup_size += (*it).second.getDedupSize();
        //total_blocks += (*it).second.getTotalBlocks();
        total_dedup_blocks += (*it).second.getDedupBlocks();
        //total_segments += (*it).second.getTotalSegments();
        //pr_msg("%s", (*it).second.ToString().c_str());
    }
            theory_dedup_blocks = (uint64_t)theoreticalIndex.size();
            cout << "Current Summary: " << endl;
    cout << "Bins: " << binmap.size() << endl;
    cout << "Segments: " << total_segments << endl;
    cout << "Global Size: " << total_size << endl;
    cout << "Global Dedup Size: " << total_dedup_size << endl;
    cout << "Global Dedup Size Ratio: " << ((double)total_dedup_size / (double)total_size)*100 << "%" << endl;
    cout << "Global Blocks: " << total_blocks << endl;
    cout << "Global Dedup Blocks: " << total_dedup_blocks << endl;
    cout << "Global Dedup Blocks Ratio: " << ((double)total_dedup_blocks / (double)total_blocks)*100 << "%" << endl;
    cout << "Theoretical Dedup Size: " << theory_dedup_size << endl;
    cout << "Theoretical Dedup Size Ratio: " << ((double)theory_dedup_size / (double)total_size)*100 << "%" << endl;
    cout << "Theoretical Dedup Blocks: " << theory_dedup_blocks << endl;
    cout << "Theoretical Dedup Blocks Ratio: " << ((double)(theory_dedup_blocks) / (double)total_blocks)*100 << "%" << endl;
    cout << "Percent of Theoretical Blocks Deduped: " << ((double)(total_blocks - total_dedup_blocks) / (double)(total_blocks - theory_dedup_blocks))*100 << "%" << endl;
            trace_index++;
            vmtraceindex++;
        }
        if (!use_trace_file) {
            break;
        }
        //vm_blocks.clear(); //clear the set for the next vm
        vmindex++;
    }

    num_bins = binmap.size();
    total_dedup_size = 0;
    total_dedup_blocks = 0;
    for (it=binmap.begin(); it != binmap.end(); it ++) {
        //total_size += (*it).second.getTotalSize();
        total_dedup_size += (*it).second.getDedupSize();
        //total_blocks += (*it).second.getTotalBlocks();
        total_dedup_blocks += (*it).second.getDedupBlocks();
        //total_segments += (*it).second.getTotalSegments();
        //pr_msg("%s", (*it).second.ToString().c_str());
    }
    //binmap.clear();

            theory_dedup_blocks = (uint64_t)theoreticalIndex.size();
            cout << "Final Summary: " << endl;
    cout << "Bins: " << binmap.size() << endl;
    cout << "Segments: " << total_segments << endl;
    cout << "Global Size: " << total_size << endl;
    cout << "Global Dedup Size: " << total_dedup_size << endl;
    cout << "Global Dedup Size Ratio: " << ((double)total_dedup_size / (double)total_size)*100 << "%" << endl;
    cout << "Global Blocks: " << total_blocks << endl;
    cout << "Global Dedup Blocks: " << total_dedup_blocks << endl;
    cout << "Global Dedup Blocks Ratio: " << ((double)total_dedup_blocks / (double)total_blocks)*100 << "%" << endl;
    cout << "Theoretical Dedup Size: " << theory_dedup_size << endl;
    cout << "Theoretical Dedup Size Ratio: " << ((double)theory_dedup_size / (double)total_size)*100 << "%" << endl;
    cout << "Theoretical Dedup Blocks: " << theory_dedup_blocks << endl;
    cout << "Theoretical Dedup Blocks Ratio: " << ((double)(theory_dedup_blocks) / (double)total_blocks)*100 << "%" << endl;
    cout << "Percent of Theoretical Blocks Deduped: " << ((double)(total_blocks - total_dedup_blocks) / (double)(total_blocks - theory_dedup_blocks))*100 << "%" << endl;
    ////cout << "Percent of Theoretical Size Deduped: " << (double)(total_size - total_dedup_size) / (double)(total_size - theory_dedup_size) << "%" << endl;
    return 0;
}
