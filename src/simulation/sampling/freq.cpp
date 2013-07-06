/*
 * Read a snapshot recipe, dedup it against its parent snapshot and CDS
 * This simulation uses lru strategy at level 2
 */
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <set>
#include <exception>
#include "dedup_types.hpp"
#include "lru_cache.hpp"

using namespace std;

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
                "  --containers - do container freq instead of block freqs\n"
                "  --rand <seg_thresh> <blk_thresh> <rand loops>\n"
                "  --rseed <seed>\n"
                "  --cds <ratio> - generate cds in program\n"
                "  --cdsevery <count> - only generate cds every <count> traces\n"
                "  --cdsfile <path>\n"
                /*"  --tracefile <path> - file containing list of traces\n"*/
                "  --traceprefix <string> - trace file paths get this prefix\n"
                "  --tracesuffix <string> - trace file paths get this suffix\n"
                "  --vmlistfile <path> - file containing list of tracefiles for VMs\n"
                "  --  - indicates end of paramers (remaining args are trace paths\n"
                );
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

//loads a segment and makes random modifications
bool load_rand_segment(Segment& seg, ifstream& is, double blk_threshold, double seg_threshold)
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

void theoretical(std::vector<Segment>& indexBlocks, ifstream& trace, double blk_threshold, double seg_threshold, unsigned int seed)
{
    //trace.clear();
    //trace.seekg(0,std::ios::beg);
    if (blk_threshold > 0 || seg_threshold > 0)
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
    while(load_rand_segment(current_seg, trace, blk_threshold, seg_threshold)) {
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

void min_heapify(int *heap, int len, int i)
{
    int left,right,smallest,t;
    do {
        left = (i << 1) + 1;
        right = (i << 1) + 2;
        smallest = i;
        if (left < len && heap[left] < heap[smallest])
            smallest = left;
        if (right < len && heap[right] < heap[smallest])
            smallest = right;
        if (smallest != i) {
            t = heap[i];
            heap[i] = heap[smallest];
            heap[smallest] = t;
            i = smallest;
        } else {
            break;
        }
    } while (i < ((len - 1) & (~1)));
}

void delete_min(int* heap, int *len)
{
    if (*len < 1)
        return;
    (*len)--;
    if (*len > 0)
    {
        heap[0] = heap[*len];
        min_heapify(heap,*len,0);
    }
}

void insert_min(int *heap, int *len, int n)
{
    int i = *len;
    int parent = (i - 1) >> 1;
    int t;
    heap[i] = n;
    while(i > 0 && heap[parent] > heap[i]) {
        t = heap[i];
        heap[i] = heap[parent];
        heap[parent] = t;
        i = parent;
        parent = (i - 1) >> 1;
    }
    (*len)++;
}

void topk_heap(int* heap, int* cur_length, int max_length, int n)
{
    if (*cur_length >= max_length)
        delete_min(heap,cur_length);
    insert_min(heap,cur_length, n);
}

void do_block_freq(map<Hash, int>& block_freq, set<Hash>& vm_blocks, ifstream& trace, map<Hash, int>& cds, double cds_percent, double blk_threshold, double seg_threshold, unsigned int seed) {
    //trace.clear();
    //trace.seekg(0,std::ios::beg);
    if (blk_threshold > 0 || seg_threshold > 0)
        srand(seed);
    map<Hash, int>::iterator iter;
    Segment current_seg;
    Hash current_hash;
    int i;
    long int blocks = 0;
    int newBlocks = 0;
    int uniqBlocks = 0;
    //printf("segments: %d\nblocks: %d\n",segments,blocks);
    while(load_rand_segment(current_seg, trace, blk_threshold, seg_threshold)) {
        for (i = 0; i < current_seg.blocklist_.size(); i++) {
            current_hash.setHash(current_seg.blocklist_[i]);
            if (vm_blocks.find(current_hash) == vm_blocks.end()) {
                vm_blocks.insert(current_hash);
                block_freq[current_hash]++;
                //for now I am ignoring the cds count, but it may come in handy later
                /*if (cds_percent < 0 && cds.find(current_hash) != cds.end()) {
                    cds[current_hash]++;
                }*/
                //if we just added this hash for the first time
                if (block_freq[current_hash] == 1)
                    newBlocks++;
                uniqBlocks++;
            }
            blocks++;
        }
        //printf("chunk finished (hits=%d;misses=%d)\n",hits,misses);
    }

    //pr_msg("trace block frequency scan finished");
    printf("New index size: %d\n", block_freq.size());
    printf("Blocks read: %lld\nUnique blocks read: %d\nNew blocks read: %d\n",blocks, uniqBlocks,newBlocks);
    unsigned long int sum = 0;
    int max = 0;
    int entries = 0;
    if (cds_percent < 0) {
        unsigned long int cds_sum = 0;
        int cds_hits = 0;

        for(iter = block_freq.begin(); iter != block_freq.end(); ++iter) {
            int count = iter->second;
            map<Hash, int>::iterator cds_iter = cds.find(iter->first);
            if (cds_iter != cds.end()) {
                cds_sum += (unsigned long int)count;
                cds_hits++;
            }
            if (count > max) {
                max = count;
            }
            sum += (unsigned long int)count;
            entries++;
            //printf("%d\n",count);
        }
        printf("Max links: %d\nCDS size: %d\nCDS blocks used: %d\n",max,cds.size(),cds_hits);
        printf("Avg links (including CDS blocks): %g\n", (double)sum / (double)entries);
        printf("Avg links (excluding CDS blocks): %g\n", (double)(sum-cds_sum) / (double)(entries - cds_hits));
        printf("Avg CDS block links: %g\n", (double)cds_sum / (double)cds.size());

    } else if (cds_percent > 0) {
        int cds_size = (int)((double)(block_freq.size()) * cds_percent);
        int cds_len = 0;
        unsigned long int cds_sum = 0;
        int *cds_entries = (int*)malloc(sizeof(int)*cds_size);

        for(iter = block_freq.begin(); iter != block_freq.end(); ++iter) {
            int count = iter->second;
            topk_heap(cds_entries, &cds_len, cds_size, count);
            sum += (unsigned long int)count;
            entries++;
            //printf("%d\n",count);
        }
        for(i = 0; i < cds_len; i++) {
            cds_sum += (unsigned long int)cds_entries[i];
            if (cds_entries[i] > max) {
                max = cds_entries[i];
            }
        }
        printf("Max links: %d\nCDS entries: %d\n",max,cds_len);
        free(cds_entries);
        printf("Avg links (including CDS blocks): %g\n", (double)sum / (double)entries);
        printf("Avg links (excluding CDS blocks): %g\n", (double)(sum-cds_sum) / (double)(entries - cds_len));
        printf("Avg CDS block links: %g\n", (double)cds_sum / (double)cds_len);
    } else {
        int maxCount = 0;
        for(iter = block_freq.begin(); iter != block_freq.end(); ++iter) {
            int count = iter->second;
            if (count > max) {
                max = count;
                maxCount = 1;
            } else if (count == max) {
                maxCount++;
            }
            sum += (unsigned long int)count;
            entries++;
            //printf("%d\n",count);
        }
        printf("Max: %d\nMax Entries: %d\nAvg: %g\n", max, maxCount, (double)sum / (double)entries);
    }
}

void do_container_freq(map<Hash, int>& container_index, set<int>& vm_containers, int &cds_containers, ifstream& trace, map<Hash, int>& cds, int &container_num, int &container_offset, int &cds_container_num, int &cds_container_offset, int container_capacity, bool use_cds, double blk_threshold, double seg_threshold, unsigned int seed) {
    //trace.clear();
    //trace.seekg(0,std::ios::beg);
    if (blk_threshold > 0 || seg_threshold > 0)
        srand(seed);
    map<Hash, int>::iterator iter;
    Segment current_seg;
    Hash current_hash;
    int current_container;
    int i;
    long int blocks = 0;
    //printf("segments: %d\nblocks: %d\n",segments,blocks);
    while(load_rand_segment(current_seg, trace, blk_threshold, seg_threshold)) {
        for (i = 0; i < current_seg.blocklist_.size(); i++) {
            current_hash.setHash(current_seg.blocklist_[i]);
            iter = container_index.find(current_hash);
            if (iter == container_index.end()) {
                if (use_cds && cds.find(current_hash) != cds.end()) {
                    current_container = cds_container_num;
                    if (++cds_container_offset >= container_capacity) {
                        cds_container_num--;
                        cds_container_offset = 0;
                    }
                } else {
                    current_container = container_num;
                    if (++container_offset >= container_capacity) {
                        container_num++;
                        container_offset = 0;
                    }
                }
                container_index[current_hash] = current_container;
            } else {
                current_container = iter->second;
            }
            if (vm_containers.find(current_container) == vm_containers.end()) {
                vm_containers.insert(current_container);
                if (current_container < 0) {
                    cds_containers++;
                }
            }
            blocks++;
        }
        //printf("chunk finished (hits=%d;misses=%d)\n",hits,misses);
    }

    //pr_msg("trace block frequency scan finished");
    printf("New index size: %d\n", container_index.size());
    printf("Total Non-CDS Containers: %d\nTotal CDS Containers: %d\n", container_num + (container_offset > 0), (-cds_container_num) - 1 + (cds_container_offset > 0));
    printf("Blocks read: %lld\nContainers used: %d\n",blocks, vm_containers.size());
    if (use_cds) {
        printf("CDS containers used: %d\nNon-CDS containers used: %d\n",cds_containers, vm_containers.size()-cds_containers);
    }
    /*if (use_cds) {
        int cds_containers = 0;
        std::set<int>::iterator container_iter;
        for(container_iter = vm_containers.begin(); container_iter != vm_containers.end(); ++container_iter) {
            if (*container_iter < 0) {
                cds_containers++;
            }
        }
        printf("CDS containers used: %d\nNon-CDS containers used: %d\n",cds_containers, vm_containers.size()-cds_containers);
    }*/
}

void sampled(std::vector<Segment>& indexBlocks, ifstream& trace, int cache_size, double blk_threshold, double seg_threshold, unsigned int seed, bool do_dirty, bool do_parent)
{
    trace.clear();
    trace.seekg(0,std::ios::beg);
    if (blk_threshold > 0 || seg_threshold > 0)
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
    while(/*t_count > 0 && */load_rand_segment(current_seg, trace, blk_threshold, seg_threshold)) {
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
    printf("cache hits: %d\n",cacheHits);
    printf("index hits: %d\n",indexHits);
    if(do_dirty) printf("clean segment blocks: %d\n",cleanSegments);
    if(do_parent) printf("parent Hits: %d\n",parentHits);
    printf("dedup misses: %d\n",misses);
    printf("dedup ratio = %d/%d (%3.3f%%)\n",hits,blocks,(double)(100*hits)/(double)blocks);
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
    //ofstream output;
    uint32_t i, j, k;
    //bool isdup;
    Block blk;
    int blockIndex;
    map<Block, int> index;
    map<Block, int>::iterator index_it;
    map<Hash, int> block_freq;
    map<Hash, int> cds;
    set<Hash> vm_blocks;
    set<int> vm_containers;
    //uint64_t new_size = 0;

    //if (argc < 4 || argc > 5) {
        //usage(argv[0]);
        //return 0;
    //}
    int argindex = 1;
    int container_index = 0;
    int container_offset = 0;
    int cds_container_index = -1;
    int cds_container_offset = 0;
    int container_capacity = (64*1024)/4;

    int vmtraceindex = 0;
    int vmindex = 0;

    bool do_dirty = false;
    bool do_parent = false;
    bool do_rand = false;
    bool do_containers = false;
    bool use_trace_file = false;
    bool use_cds_file = false;
    bool use_vm_list = false;
    double blk_threshold = 0;
    double seg_threshold = 0;
    double cds_percent = 0;
    int cds_every = 1;
    int trace_index = 0;
    unsigned int seed;
    unsigned int loops = 1;
    unsigned int loopindex = 0;

    string tracefile_prefix, tracefile_suffix;

    while(argindex < argc && argv[argindex][0] == '-')
    {
        if (strcmp(argv[argindex],"--dirty") == 0) {
            do_dirty = true;
            argindex++;
        } else if (strcmp(argv[argindex],"--parent") == 0) {
            do_parent = true;
            argindex++;
        } else if (strcmp(argv[argindex],"--containers") == 0) {
            do_containers = true;
            argindex++;
        } else if (strcmp(argv[argindex],"--rand") == 0) {
            do_rand = true;
            argindex++;
            if ((argindex+2) >= argc) {
                usage(argv[0]);
                return 0;
            }
            seg_threshold = atof(argv[argindex++]);
            blk_threshold = atof(argv[argindex++]);
            loops = (unsigned int)atol(argv[argindex++]) + 1;
        } else if (strcmp(argv[argindex],"--rseed") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            seed = (unsigned int)atol(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--cds") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            cds_percent = atof(argv[argindex++]);
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
        } else if (strcmp(argv[argindex],"--cdsevery") == 0) {
            argindex++;
            if (argindex >= argc) {
                usage(argv[0]);
                return 0;
            }
            cds_every = atoi(argv[argindex++]);
        } else if (strcmp(argv[argindex],"--tracefile") == 0) {
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
    
    int start_argindex = argindex;

    while(loopindex < loops) {
        loopindex++;
        double tseg_threshold = 0;
        double tblk_threshold = 0;
        if (loopindex > 1) {
            //reset the streams in case we have already looped through at least once
            if (use_vm_list) {
                vmlist_input.clear();
                vmlist_input.seekg(0,ios::beg);
            } else if (use_trace_file) {
                trace_input.clear();
                trace_input.seekg(0,ios::beg);
            } else {
                argindex = start_argindex;
            }
            tseg_threshold = seg_threshold;
            tblk_threshold = blk_threshold;
        }
        if (tseg_threshold > 0 && tblk_threshold > 0) {
            cout << "Beggining loop " << (loopindex -1) << " with seed " << seed << endl;
        } else {
            cout << "Beggining loop " << (loopindex -1) << " with no rand" << endl;
        }

        while(!use_trace_file || trace_input.good() || vmlist_input.good()) {
            string line;
            const char *cur_vm_path;
            int cds_containers = 0;
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
                double temp_cds_percent;
                if (use_trace_file) {
                    getline(trace_input,cur_tracefile_name);
                } else {
                    cur_tracefile_name = argv[argindex++];
                }
                tracepath_stream << tracefile_prefix << cur_tracefile_name << tracefile_suffix;
                cur_trace_path = tracepath_stream.str().c_str();
                if (cur_tracefile_name.length() < 1 || cur_trace_path[0] == '\n') {
                    continue;
                }

                    if (current_input.is_open()) {
                        current_input.close();
                    }
                    current_input.open(cur_trace_path, std::ios_base::in | std::ios_base::binary);
                    if (!current_input.is_open()) {
                        pr_msg("unable to open %s", cur_trace_path);
                        exit(1);
                    }
                    printf("snapshot trace file %d.%d: %s\n", vmindex, vmtraceindex,cur_trace_path);
                    //theoretical(indexBlocks, current_input, tblk_threshold, seed);
                    //sampled(indexBlocks, current_input, cache_size, tblk_threshold, seed, do_dirty, do_parent);
                    if (do_containers) {
                        do_container_freq(block_freq, vm_containers, cds_containers, current_input,
                            cds, container_index, container_offset,
                            cds_container_index, cds_container_offset,
                            container_capacity, use_cds_file, tblk_threshold, tseg_threshold, seed);
                        if (!use_vm_list) {
                            vm_containers.clear();
                            cds_containers = 0;
                        }
                    } else {
                        if (use_cds_file) {
                            temp_cds_percent = -1;
                        } else if ((trace_index % cds_every) == 0) {
                            temp_cds_percent = cds_percent;
                        } else {
                            temp_cds_percent = 0;
                        }
                        do_block_freq(block_freq, vm_blocks, current_input, cds, temp_cds_percent, tblk_threshold, tseg_threshold, seed);
                        if (!use_vm_list) {
                            vm_blocks.clear();
                        }
                    }

                    //output.close();
                    //parent_input.close();
                    current_input.close();
                trace_index++;
                vmtraceindex++;
            }
            if (!use_trace_file) {
                break;
            }
            if (do_containers) {
                if (use_vm_list) {
                    printf("VM summary (%d) Total Non-CDS Containers: %d\n", vmindex, container_index + (container_offset > 0));
                    printf("VM summary (%d) Total CDS Containers: %d\n", vmindex, (-cds_container_index) - 1 + (cds_container_offset > 0));
                    printf("VM summary (%d) Containers used: %d\n", vmindex, vm_containers.size());
                    if (use_cds_file) {
                        printf("VM summary (%d) CDS containers used: %d\n",vmindex, cds_containers);
                        printf("VM summary (%d) Non-CDS containers used: %d\n", vmindex, vm_containers.size()-cds_containers);
                    }
                }
                vm_containers.clear();
                cds_containers = 0;
            } else {
                map<Hash, int>::iterator lnkit;
                unsigned long int sum = 0; 
                unsigned long int entries = 0;
                for(lnkit = block_freq.begin(); lnkit != block_freq.end(); ++lnkit) {
                    int count = lnkit->second;
                    sum += (unsigned long int)count;
                    entries++;
                }
                printf("VM summary (%d) Avg blk links: %g\n", vmindex, (double)sum / (double)entries);
                vm_blocks.clear(); //clear the set for the next vm
            }
            vmindex++;
        }
        seed = seed * 13 + 1;
    }
    return 0;
}
