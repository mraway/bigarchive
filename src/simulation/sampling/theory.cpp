/*
 * Read a snapshot recipe, dedup it against its parent snapshot and CDS
 * This simulation uses lru strategy at level 2
 */
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <exception>
#include "dedup_types.hpp"

using namespace std;

void usage(char *progname)
{
	pr_msg("This program read a snapshot recipe, dedup and dedups it.\n"
           "It uses the High Performance Deduplication System paper algorithm\n"
	       "Usage: %s CDS cache_size snapshot [parent]", progname);
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
            pr_msg("ignore zero-sized block");
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

int main(int argc, char** argv)
{
    int cacheHits = 0;
    int indexHits = 0;
    int misses = 0;

    //std::vector<Block> cds;
    std::vector<Block>::iterator it;
    Segment current_seg;
    ifstream current_input, index_input, cds_input;
    ofstream output;
    uint32_t i, j, k;
    bool isdup;
    Block blk;
    int blockIndex;
    map<Block, int> index;
    map<Block, int>::iterator index_it;
    //uint64_t new_size = 0;

    if (argc < 3 || argc > 4) {
        usage(argv[0]);
        return 0;
    }

    current_input.open(argv[1], std::ios_base::in | std::ios_base::binary);
    if (!current_input.is_open()) {
        pr_msg("unable to open %s", argv[1]);
        exit(1);
    }
    //cds_input.open(argv[2], std::ios_base::in | std::ios_base::binary);
    //if (!cds_input.is_open()) {
        //pr_msg("unable to open %s", argv[2]);
        //exit(1);
    //}

    index_input.open(argv[2], std::ios_base::in | std::ios_base::binary);
    if (!index_input.is_open()) {
        pr_msg("unable to open %s", argv[2]);
        exit(1);
    }

    //string outputname = argv[3];
    //outputname += ".simlru";
    //outputname += argv[2];
    //output.open(outputname.c_str(), std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

    // prepare CDS, assume it was sorted
    //while (blk.Load(cds_input)) {
        //cds.push_back(blk);
        ////sort(cds.begin(), cds.end());
    //}
    //cds_input.close();

    //first load the index with the entire index snapshot
    //we will only perform sampled checks against it, but we need all of it
    int segments = 0;
    int blocks = 0;
    i = 0;
    while(load_segment(current_seg, index_input)) {
        for(k = 0; k < current_seg.blocklist_.size(); k++)
            index[current_seg.blocklist_[k]] = segments & (~7);
        segments++;
        blocks += current_seg.blocklist_.size();
    }
    printf("segments: %d\nblocks: %d\n",segments,blocks);

    //for each segment in the trace
    //int segi = 0;
    //int t_count = 10;
    while(/*t_count > 0 && */load_segment(current_seg, current_input)) {
        //t_count--;
        //cache.AddItems(indexBlocks[segi++]);
        // prepare current segment, 0 means non-dup
        //for (i = 0; i < current_seg.blocklist_.size(); i ++)
            //current_seg.blocklist_[i].file_id_ = 0;
        
        //for each block in the segment
        for (i = 0; i < current_seg.blocklist_.size(); i++)
        {
            //first check if it is in the cache

            //next check in the index, if we get a hit cache the block
            isdup = false;
            index_it = index.find(current_seg.blocklist_[i]);
            if (index_it != index.end())
            {
                indexHits++;
            }
            else
                misses++;
        }
        printf("chunk finished (c=%d;i=%d;m=%d)\n",cacheHits,indexHits,misses);
    }

    pr_msg("dedup finish");
    printf("cache Hits: %d\n",cacheHits);
    printf("index Hits: %d\n",indexHits);
    printf("dedup Misses: %d\n",misses);

    //output.close();
    //parent_input.close();
    current_input.close();
    return 0;
}
