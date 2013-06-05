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
#include "lru_cache.hpp"

using namespace std;

void usage(char *progname)
{
    pr_msg("This program read a snapshot recipe and dedups it against another trace.\n"
        "It uses the High Performance Deduplication System paper algorithm (sampling)\n"
        "Usage: %s [--dirty] [--parent] [--rand <change_ratio>]\n"
        "    cache_size snapshot [parent]", progname);
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
            pr_msg("ignore zero-sized block");
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

//compute the maximum dedup possible for the input data
void theoretical(std::vector<Segment>& indexBlocks, ifstream& trace, double blk_threshold, unsigned int seed)
{
    //trace.clear();
    //trace.seekg(0,std::ios::beg);
    if (blk_threshold > 0)
        srand(seed);
    map<Block, int> index;
    map<Block, int>::iterator index_it;
    Segment current_seg;
    int segments = 0;
    int blocks = 0;
    int hits = 0;
    int misses = 0;
    int i,j;
    for(i = 0; i < indexBlocks.size(); i++)
    {
        for(j = 0; j < indexBlocks[i].blocklist_.size(); j++)
            index[indexBlocks[i].blocklist_[j]] = segments & (~7); //group blocks by segment
        segments++;
        blocks += indexBlocks[i].blocklist_.size();
    }
    //printf("segments: %d\nblocks: %d\n",segments,blocks);
    while(load_rand_segment(current_seg, trace, blk_threshold)) {
        for (i = 0; i < current_seg.blocklist_.size(); i++)
        {
            index_it = index.find(current_seg.blocklist_[i]);
            if (index_it != index.end())
                hits++;
            else misses;
        }
        //printf("chunk finished (hits=%d;misses=%d)\n",hits,misses);
    }

    //pr_msg("theoretical dedup finish");
    printf("theoretical dedup ratio = %d/%d (%3.3f%%)\n",hits,blocks,(double)(100*hits)/(double)blocks);
}

//simulates the sampled index approach to deduplication (using sampled index+cache)
void sampled(std::vector<Segment>& indexBlocks, ifstream& trace, int cache_size, double blk_threshold, unsigned int seed, bool do_dirty, bool do_parent)
{
    trace.clear();
    trace.seekg(0,std::ios::beg);
    if (blk_threshold > 0)
        srand(seed);
    LruCache cache(cache_size);
    int i, j, k;
    int blockIndex;
    int indexHits = 0, cacheHits = 0, misses = 0, cleanSegments = 0, parentHits = 0;
    map<Block, int> index;
    map<Block, int>::iterator index_it;
    Segment current_seg;
    Segment parent_seg;
    //now create the sampled index
    for (i = 0; i < indexBlocks.size(); i +=8) {
        //loop through every chunk in the container, sampling at least the first block
        blockIndex = 0;
        for(j = 0; j < 8 && (i + j) < indexBlocks.size(); j++)
        {
            for (k = 0; k < indexBlocks[i+j].blocklist_.size(); k ++) {
                if ((blockIndex++) % 100 == 0)
                    index[indexBlocks[i+j].blocklist_[k]] = i;
            }
        }
    }
    //printf("segments: %d\nblocks: %d\n",segments,blocks);

    //for each segment in the trace
    int segi = 0;
    //int t_count = 10000;
    //while(t_count > 0 && load_rand_segment(current_seg, trace, blk_threshold)) {
    while(/*t_count > 0 && */load_rand_segment(current_seg, trace, blk_threshold)) {
        //t_count--;
        //cache.AddItems(indexBlocks[segi++]);
        // prepare current segment, 0 means non-dup
        //for (i = 0; i < current_seg.blocklist_.size(); i ++)
            //current_seg.blocklist_[i].file_id_ = 0;
        
        if (do_dirty)
        {
            if (current_seg == indexBlocks[segi])
            {
                cleanSegments+= current_seg.blocklist_.size();
                segi++;
                continue;
            }
        }

        if (do_parent)
        {
            parent_seg = indexBlocks[segi];
            parent_seg.SortByHash();
        }
        
        //for each block in the segment
        for (i = 0; i < current_seg.blocklist_.size(); i++)
        {
            if (do_parent)
            {
                if (parent_seg.SearchBlock(current_seg.blocklist_[i]))
                {
                    parentHits++;
                    continue;
                }
            }
            //first check if it is in the cache
            if (cache.SearchItem(current_seg.blocklist_[i])) {
                cacheHits++;
                continue;
            }

            //next check in the index, if we get a hit cache the block
            index_it = index.find(current_seg.blocklist_[i]);
            if (index_it != index.end())
            {
                j = index_it->second; //index of the first container block
                for(k = j; k < (j + 8) && k < indexBlocks.size(); k++)
                {
                    cache.AddItems(indexBlocks[k]);
                }
                indexHits++;
            }
            else
                misses++;
        }
        //printf("chunk finished (c=%d;i=%d;m=%d)\n",cacheHits,indexHits,misses);
        segi++;
    }
    int hits = cacheHits+indexHits+cleanSegments+parentHits;
    int blocks = hits + misses;

    pr_msg("dedup finish");
    printf("cache Hits: %d\n",cacheHits);
    printf("index Hits: %d\n",indexHits);
    if(do_dirty) printf("clean segment blocks: %d\n",cleanSegments);
    if(do_parent) printf("parent Hits: %d\n",parentHits);
    printf("dedup Misses: %d\n",misses);
    printf("dedup ratio = %d/%d (%3.3f%%)\n",hits,blocks,(double)(100*hits)/(double)blocks);
}

int main(int argc, char** argv)
{
    //int cacheHits = 0;
    //int indexHits = 0;
    //int misses = 0;

    //std::vector<Block> cds;
    std::vector<Segment> indexBlocks;
    //std::vector<Block>::iterator it;
    Segment current_seg;
    ifstream current_input, index_input, cds_input;
    //ofstream output;
    uint32_t i, j, k;
    //bool isdup;
    Block blk;
    int blockIndex;
    map<Block, int> index;
    map<Block, int>::iterator index_it;
    //uint64_t new_size = 0;

    //if (argc < 4 || argc > 5) {
        //usage(argv[0]);
        //return 0;
    //}
    int argindex = 1;
    bool do_dirty = false;
    bool do_parent = false;
    bool do_rand = false;
    bool do_theory = true;
    double blk_threshold = 0;
    unsigned int seed;

    while(argindex < argc && argv[argindex][0] == '-')
    {
        if (strcmp(argv[argindex],"--dirty") == 0)
        {
            do_dirty = true;
            argindex++;
        }
        else if (strcmp(argv[argindex],"--parent") == 0)
        {
            do_parent = true;
            argindex++;
        }
        else if (strcmp(argv[argindex], "--notheory") == 0)
        {
            do_theory = false;
            argindex++;
        }
        else if (strcmp(argv[argindex],"--rand") == 0)
        {
            do_rand = true;
            argindex++;
            if ((argindex+1) >= argc)
            {
                usage(argv[0]);
                return 0;
            }
            blk_threshold = atof(argv[argindex++]);
            seed = (unsigned int)atol(argv[argindex++]);
        }
    }

    if ( (!do_rand && argindex +2 >= argc) || argindex +1 >= argc)
    {
        usage(argv[0]);
        return 0;
    }

    int cache_size = atoi(argv[argindex++]);
    char *cur_trace_path = argv[argindex++];
    char *index_trace_path;
    if (!do_rand)
        index_trace_path = argv[argindex++];
    else
        index_trace_path = cur_trace_path;

    current_input.open(cur_trace_path, std::ios_base::in | std::ios_base::binary);
    if (!current_input.is_open()) {
        pr_msg("unable to open %s", cur_trace_path);
        exit(1);
    }
    //cds_input.open(argv[2], std::ios_base::in | std::ios_base::binary);
    //if (!cds_input.is_open()) {
        //pr_msg("unable to open %s", argv[2]);
        //exit(1);
    //}


    index_input.open(index_trace_path, std::ios_base::in | std::ios_base::binary);
    if (!index_input.is_open()) {
        pr_msg("unable to open %s", index_trace_path);
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
    indexBlocks.reserve(3000000);
    while(load_segment(current_seg, index_input)) {
        indexBlocks.push_back(current_seg);
        segments++;
        blocks += current_seg.blocklist_.size();
    }

    if (do_theory)
        theoretical(indexBlocks, current_input, blk_threshold, seed);
    sampled(indexBlocks, current_input, cache_size, blk_threshold, seed, do_dirty, do_parent);

    //output.close();
    //parent_input.close();
    current_input.close();
    return 0;
}
