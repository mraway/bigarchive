#include "../snapshot/trace_types.h"
#include <map>
#include <iostream>
#include <fstream>
#include <string.h>
#include "../common/timer.h"

using namespace std;

struct SizeCount {
    SizeCount() { size_ = 0; count_ = 0; };
    uint32_t size_;
    uint32_t count_;
};

int main( int argc, char** argv)
{
    if (argc != 3) {
	cout << "Usage: " <<argv[0] << " trace_file num_bits" <<endl;
	return 0;
    }

    DOMConfigurator::configure("Log4cxxConfig.xml");
    TIMER_START();

    map<Checksum, SizeCount> ublks;
    ifstream is(argv[1], ios_base::in | ios_base::binary);
    Block blk;
    int nbits = atoi(argv[2]);
    uint32_t num_buckets = 0x1 << (nbits);
    uint64_t* raw_sizes = new uint64_t[num_buckets];
    uint64_t* dedup_sizes = new uint64_t[num_buckets];
    uint64_t* unique_blocks = new uint64_t[num_buckets];
    uint64_t* total_blocks = new uint64_t[num_buckets];

    for (unsigned int i = 0; i < num_buckets; ++i)
    {
        raw_sizes[i] = dedup_sizes[i] = unique_blocks[i] = total_blocks[i] = 0;
    }

    cout << "Use the first " << nbits << " bits, " << num_buckets << " buckets" << endl;
    
    while (blk.FromStream(is)) {
        ublks[blk.cksum_].count_ ++;
        ublks[blk.cksum_].size_ = blk.size_;
    }

    map<Checksum, SizeCount>::iterator it;
    for (it = ublks.begin(); it != ublks.end(); it++) {
        uint32_t first4 = it->first.First4Bytes();
        uint32_t bucket_id = first4 >> (32 - nbits);
        raw_sizes[bucket_id] += it->second.size_ * it->second.count_;
        dedup_sizes[bucket_id] += it->second.size_;
        unique_blocks[bucket_id]++;
        total_blocks[bucket_id] += it->second.count_;
    }

    for (unsigned int i = 0; i < num_buckets; ++i)
    {
        cout << "Bucket " << i << 
            ": raw " << raw_sizes[i] << 
            ", dedup " << dedup_sizes[i] <<
            ", unique " << unique_blocks[i] <<
            ", total " << total_blocks[i] << endl;
    }

    TIMER_PRINT_ALL();

    delete[] raw_sizes;
    delete[] dedup_sizes;
    delete[] unique_blocks;
    delete[] total_blocks;
    is.close();
    exit(0);
}


















