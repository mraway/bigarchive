#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <set>
#include <exception>
#include "../sampling/dedup_types.hpp"

//AA??????????????????a.1000-????-ttttt-full.vhd.bv4

void usage(char *progname)
{
	pr_msg("This program computes block and container frequencies for\n"
                "dedup using various settings. Some parameters are copied\n"
                "from other simulation programs so you may have to look at\n"
                "the code to find which parameters are valid.\n"
	        "Usage: %s <parameters>", progname);
        pr_msg("Parameters may be one of:\n"
                /*"  --dirty\n"
                "  --parent\n"*/
                "  --segthresh <segment threshold>\n"
                "  --blkthresh <block threshold>\n"
                "  --rseed <seed>\n"
                "  --cdsfile <path>\n"
                "  --outputdir <path> - traceprefix is prepended\n"
                /*"  --tracefile <path> - file containing list of traces\n"*/
                "  --traceprefix <string> - trace file paths get this prefix\n"
                "  --tracesuffix <string> - trace file paths get this suffix\n"
                "  --vmlistfile <path> - file containing list of tracefiles for VMs\n"
                /* "  --  - indicates end of paramers (remaining args are trace paths\n" */
                );
}

class Hash {
    private:
    Checksum cksum_;
    public:
    Hash() {
    }
    Hash(Block& blk) {
        memcpy(cksum_, blk.cksum_, CKSUM_LEN * sizeof(uint8_t));
    }
    Hash(const Hash& h)
    {
        memcpy(cksum_, h.cksum_, CKSUM_LEN * sizeof(uint8_t));
    }
    void setHash(Block& blk) {
        memcpy(cksum_, blk.cksum_, CKSUM_LEN * sizeof(uint8_t));
    }
    bool operator==(const Hash& other) const {
        return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) == 0;
    }

    bool operator!=(const Hash& other) const {
        return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) != 0;
    }

    bool operator<(const Hash& other) const {
        return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) < 0;
    }
};

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
    //seg.Final();
    if (seg.GetSize() == 0 || seg.blocklist_.size() == 0) 
        return false;
    else
        return true;
}

//loads a segment and makes random modifications
bool load_rand_segment(Segment& seg, ifstream& is, double blk_threshold, double seg_threshold, unsigned long int &seg_changed, unsigned long int &blk_changed)
{
    if (blk_threshold == 0 || seg_threshold == 0)
        return load_segment(seg,is);
    if (((double)rand() / RAND_MAX) > seg_threshold)
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
            blk_changed++;
            rd = (double)rand() / RAND_MAX;
            memcpy(&blk.cksum_[4],&rd,sizeof(rd));
        }
        seg.AddBlock(blk);
        if (seg.GetSize() >= 2*1024*1024)
            break;
    }
    //seg.Final();
    if (seg.GetSize() == 0 || seg.blocklist_.size() == 0) 
        return false;
    else {
        seg_changed++;
        return true;
    }
}

int main(int argc, char** argv)
{
    //int cacheHits = 0;
    //int indexHits = 0;
    //int misses = 0;

    //std::vector<Block> cds;
    //std::vector<Segment> indexBlocks;
    //std::vector<Block>::iterator it;
    Segment current_seg;
    ifstream current_input, trace_input, cds_input, vmlist_input;
    ofstream current_output;
    //ofstream output;
    uint32_t i, j, k;
    //bool isdup;
    Block blk;
    map<Hash, int> cds;
    //uint64_t new_size = 0;

    //if (argc < 4 || argc > 5) {
        //usage(argv[0]);
        //return 0;
    //}
    int argindex = 1;

    int vmtraceindex = 0;
    int vmindex = 0;

    bool do_containers = false;
    bool use_cds_file = false;
    bool use_vm_list = false;
    double blk_threshold = 0;
    double seg_threshold = 0;
    double tblk_threshold = 0;
    double tseg_threshold = 0;
    int cds_every = 1;
    int trace_index = 0;
    unsigned int seed;
    unsigned int tseed;
    unsigned int loops = 1;
    unsigned int loopindex = 0;
    unsigned int start_loop = 0;
    unsigned int snapshot_count = 1;
    unsigned int snapshot_index = 0;
    unsigned long int vmid = 0;

    string tracefile_prefix, tracefile_suffix, output_dir;

    while(argindex < argc && argv[argindex][0] == '-')
    {
        if (strcmp(argv[argindex],"--segthresh") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            seg_threshold = atof(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--blkthresh") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            blk_threshold = atof(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--tsegthresh") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            tseg_threshold = atof(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--tblkthresh") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            tblk_threshold = atof(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--loops") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            loops = (unsigned int)atol(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--startloop") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            start_loop = (unsigned int)atol(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--snapshots") == 0) {
            argindex++;
            if ((argindex) >= argc) {
                usage(argv[0]);
                return 0;
            }
            snapshot_count = (unsigned int)atol(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--rseed") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            seed = (unsigned int)atol(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--cdsfile") == 0) {
            use_cds_file = true;
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            cds_input.open(argv[argindex++], std::ios_base::in);
            if (!cds_input.is_open()) {
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
        } else if (strcmp(argv[argindex],"--outputdir") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            output_dir = argv[argindex++];
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

    //If we will be using a premade cds, load it into memory
    if (use_cds_file) {
        Hash current_hash;
        while(load_segment(current_seg, cds_input)) {
            for (i = 0; i < current_seg.blocklist_.size(); i++) {
                current_hash.setHash(current_seg.blocklist_[i]);
                cds[current_hash] = 0;
            }
        }
    }
    
    loopindex = start_loop;

    while(loopindex < loops) {
        if (loopindex > 0) {
            //reset the stream
            vmlist_input.clear();
            vmlist_input.seekg(0,ios::beg);
        }
        cout << "Beggining loop " << loopindex << endl;
        vmid = (unsigned long int)1009 * (unsigned long int)(loopindex+1);

        while(vmlist_input.good()) {
            string line;
            const char *cur_vm_path;
            int cds_containers = 0;
            vmtraceindex = 0;
            snapshot_index = 0;

            getline(vmlist_input,line);
            cur_vm_path = line.c_str();
            if (strlen(cur_vm_path) < 1 || cur_vm_path[0] == '\n') {
                continue;
            }
            if (trace_input.is_open()) {
                trace_input.close();
            }
            cout << "VM file (" << vmindex << "): " << cur_vm_path << endl;
            trace_input.open(cur_vm_path, std::ios_base::in);
            if (!trace_input.is_open()) {
                pr_msg("unable to open %s", cur_vm_path);
                exit(1);
            }

            while(trace_input.good()) {
                unsigned long int seg_changed = 0;
                unsigned long int blk_changed = 0;
                unsigned long int seg_count = 0;
                unsigned long int blk_count = 0;
                const char *cur_trace_path;
                string cur_tracefile_name;
                stringstream tracepath_stream;
                double temp_cds_percent;

                getline(trace_input,cur_tracefile_name);

                tracepath_stream << tracefile_prefix << cur_tracefile_name << tracefile_suffix;
                cur_trace_path = tracepath_stream.str().c_str();
                if (cur_tracefile_name.length() < 1 || cur_trace_path[0] == '\n') {
                    continue;
                }

                current_input.open(cur_trace_path, std::ios_base::in | std::ios_base::binary);
                if (!current_input.is_open()) {
                    pr_msg("unable to open %s", cur_trace_path);
                    exit(1);
                }
                printf("snapshot trace file %d.%d: %s\n", vmindex, vmtraceindex,cur_trace_path);
                stringstream outpath_stream;
                outpath_stream << setfill('0');
                outpath_stream << tracefile_prefix << output_dir << "/" << setw(21) << vmid << ".1000-0000-" << setw(5) << snapshot_index << "-full.vhd" << tracefile_suffix;
                cout << "output snapshot (" << vmid << "." << snapshot_index << "): " << outpath_stream.str() << endl;
                current_output.open(outpath_stream.str().c_str(), std::ios_base::out | std::ios_base::binary);
                if (!current_output.is_open()) {
                    pr_msg("unable to open %s", outpath_stream.str().c_str());
                    exit(1);
                }



                tseed = seed * ((unsigned int)vmid) * (snapshot_index+1);
                srand(tseed);
                cout << "  Seed: " << tseed << endl;
                seg_count = 0;
                blk_count = 0;
                seg_changed = 0;
                blk_changed = 0;
                while(load_rand_segment(current_seg, current_input, blk_threshold, seg_threshold, seg_changed, blk_changed)) {
                    seg_count++;
                    blk_count += current_seg.blocklist_.size();
                    current_seg.Save(current_output);
                }
                cout << "  Changed " << seg_changed << "/" << seg_count << " Segments (" << ((100.0 * seg_changed) / seg_count) << "%)" << endl;
                cout << "  Changed " << blk_changed << "/" << blk_count << " Blocks (" << ((100.0 * blk_changed) / blk_count) << "%)" << endl;
                snapshot_index++;

                current_input.close();
                current_output.close();

                while (snapshot_index < snapshot_count) {
                    current_input.open(outpath_stream.str().c_str(), std::ios_base::in | std::ios_base::binary);
                    if (!current_input.is_open()) {
                        pr_msg("unable to open %s", outpath_stream.str().c_str());
                        exit(1);
                    }
                    outpath_stream.str(std::string());
                    outpath_stream << tracefile_prefix << output_dir << "/" << setw(21) << vmid << ".1000-0000-" << setw(5) << snapshot_index << "-full.vhd" << tracefile_suffix;
                    cout << "output snapshot (" << vmid << "." << snapshot_index << "): " << outpath_stream.str() << endl;
                    current_output.open(outpath_stream.str().c_str(), std::ios_base::out | std::ios_base::binary);
                    if (!current_input.is_open()) {
                        pr_msg("unable to open %s", outpath_stream.str().c_str());
                        exit(1);
                    }

                    tseed = seed * ((unsigned int)vmid) * (snapshot_index+1);
                    cout << "  Seed: " << tseed << endl;
                    srand(tseed);
                    seg_count = 0;
                    blk_count = 0;
                    seg_changed = 0;
                    blk_changed = 0;
                    while(load_rand_segment(current_seg, current_input, tblk_threshold, tseg_threshold, seg_changed, blk_changed)) {
                        seg_count++;
                        blk_count += current_seg.blocklist_.size();
                        current_seg.Save(current_output);
                    }
                    cout << "  Changed " << seg_changed << "/" << seg_count << " Segments (" << ((100.0 * seg_changed) / seg_count) << "%)" << endl;
                    cout << "  Changed " << blk_changed << "/" << blk_count << " Blocks (" << ((100.0 * blk_changed) / blk_count) << "%)" << endl;

                    current_input.close();
                    current_output.close();
                    snapshot_index++;
                }

                trace_index++;
                vmtraceindex++;
                break; // move on to the next VM
            }

            vmindex++;
            vmid++;
        }
        loopindex++;
    }
    return 0;
}
