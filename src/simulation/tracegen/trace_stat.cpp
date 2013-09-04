/*
 * this program split list of blocks into large segments,
 * using archor to determine the boundary of segments
 */

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <sstream>

#include "../sampling/dedup_types.hpp"

using namespace std;

void usage(char *progname)
{
    pr_msg( "This program prints basic stats about a trace file\n"
            "Usage: %s [options] tracefile1,tracefile2,...", progname);
    pr_msg( "Possible options are:\n"
            "   long     short\n"
            "  --help     -?   print this help message and exit\n"
            "  --header   -h   print header before first line\n"
            "  --dump     -d   <path> path to copy all read segments to (to append traces)\n"
            "  --size     -s   <num_bytes> limit reading (and writing) to <num_bytes> bytes\n"
            "  --blocks   -b   <num_blocks> limit reading (and writing) to <num_blocks> blocks\n"
            "  --segments -g   <num_segments> limit reading (and writing) to <num_segments> 2MB segments\n"
            "All size limits are to the nearest segment, and reading/dumping will stop after"
            " the total size is equal to or greater than the limit\n"
            "Once the size limit has been met, no more trace files will be read (the size limit is global)\n");
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
    //seg.Final();
    if (seg.GetSize() == 0 || seg.blocklist_.size() == 0) 
        return false;
    else
        return true;
}

int main(int argc, char **argv)
{
    Segment current_seg;
    ifstream current_input;
    ofstream dump_output;

    Segment seg;
    uint64_t total_size = 0;
    uint64_t total_blocks = 0;
    uint64_t total_segments = 0;
    uint64_t size_limit = 0;
    uint64_t block_limit = 0;
    uint64_t segment_limit = 0;
    bool display = true;
    bool display_total = true;
    bool dump = false;
    char* dump_path;

    int argi = 1;

    while(argi < argc && argv[argi][0] == '-')
    {
        if (strcmp(argv[argi],"--") == 0) {
            argi++;
            break;
        } else if (strcmp(argv[argi],"--help") == 0 ||
                    strcmp(argv[argi],"-?") == 0) {
            argi++;
            usage(argv[0]);
            exit(0);
        } else if (strcmp(argv[argi],"--dump") == 0 ||
                strcmp(argv[argi],"-d") == 0) {
            argi++;
            if (argi >= argc) {
                cout << "must specify dump file" << endl;
                usage(argv[0]);
                exit(1);
            }
            dump_path = argv[argi++];
            dump = true;
        } else if (strcmp(argv[argi],"--size") == 0 ||
                strcmp(argv[argi],"-s") == 0) {
            argi++;
            if (argi >= argc) {
                cout << "size argument specified but no size given" << endl;
                usage(argv[0]);
                exit(1);
            }
            size_limit = (uint64_t)atol(argv[argi++]);
        } else if (strcmp(argv[argi],"--blocks") == 0 ||
                strcmp(argv[argi],"-b") == 0) {
            argi++;
            if (argi >= argc) {
                cout << "blocks argument specified but no size given" << endl;
                usage(argv[0]);
                exit(1);
            }
            block_limit = (uint64_t)atol(argv[argi++]);
        } else if (strcmp(argv[argi],"--segments") == 0 ||
                strcmp(argv[argi],"-g") == 0) {
            argi++;
            if (argi >= argc) {
                cout << "segment argument specified but no size given" << endl;
                usage(argv[0]);
                exit(1);
            }
            segment_limit = (uint64_t)atol(argv[argi++]);
        } else if (strcmp(argv[argi],"--header") == 0 ||
                strcmp(argv[argi],"-h") == 0) {
            argi++;
            cout << "Segments\tBlocks\tBytes" << endl;
        } else {
            cout << "Invalid argument. Use -- before using filename args beginning with '-'" << endl;
            usage(argv[0]);
            exit(1);
        }
    }
    if (dump) {
        dump_output.open(dump_path, std::ios_base::out |
                std::ios_base::binary);
        if (!dump_output.is_open()) {
            pr_msg("unable to open %s for output", dump_path);
            exit(3);
        }
    }
    int snapshots = 0;
    while(argi < argc) {
        if (size_limit > 0 && total_size >= size_limit) {
            break;
        } else if (block_limit > 0 && total_blocks >= block_limit) {
            break;
        } else if (segment_limit > 0 && total_segments >= segment_limit) {
            break;
        }
        uint64_t size = 0;
        uint64_t blocks = 0;
        uint64_t segments = 0;
        const char *cur_trace_path = argv[argi++];
        if (strlen(cur_trace_path) < 1 || cur_trace_path[0] == '\n') {
            continue;
        }
        current_input.open(cur_trace_path, std::ios_base::in);
        if (!current_input.is_open()) {
            pr_msg("unable to open %s for input", cur_trace_path);
            exit(2);
        }
        while (load_segment(seg,current_input)) {
            if (size_limit > 0 && total_size >= size_limit) {
                cout << "Size limit met. Ending input." << endl;
                break;
            } else if (block_limit > 0 && total_blocks >= block_limit) {
                cout << "Block limit met. Ending input." << endl;
                break;
            } else if (segment_limit > 0 && total_segments >= segment_limit) {
                cout << "Segment limit met. Ending input." << endl;
                break;
            }
            std::vector<Block>::iterator iter;
            for(iter = seg.blocklist_.begin();iter != seg.blocklist_.end();
                    ++iter) {
		if ((*iter).size_ == 0 || (*iter).size_ > 16384) {
			cout << "corrupted block detected; block size: " << (*iter).size_ << endl;
		}
                size += (*iter).size_;
                total_size += (*iter).size_;
                blocks++;
                total_blocks++;
            }
            segments++;
            total_segments++;
            if (dump) {
                seg.Save(dump_output);
            }
        }
        current_input.close();
        snapshots++;
        if (display) {
            cout << segments << "\t" << blocks << "\t" << size << "\t" << 
                cur_trace_path << endl;
        }
    }
    if (snapshots > 1 && display_total) {
        cout << total_segments << "\t" << total_blocks << "\t" << total_size << "\ttotal" << endl;
    }
    if (dump_output.is_open()) {
        dump_output.close();
    }

    return 0;
}
