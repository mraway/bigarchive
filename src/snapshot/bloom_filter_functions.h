#ifndef _BLOOM_FILTER_FUNCTIONS_H_
#define _BLOOM_FILTER_FUNCTIONS_H_

#include "trace_types.h"
#include "string.h"

// see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
#define BLOOM_FILTER_FP_RATE 0.01
#define BLOOM_FILTER_NUM_FUNCS 7

/*
 * pick the first eight bytes start from func_id as bloom filter hash
 */
inline uint64_t hash_for_checksum(const Checksum& cksum, int start)
{
    int position = start % CKSUM_LEN;
    uint64_t val = 0;
    char* p_val = reinterpret_cast<char*>(&val);
    size_t bytes_remain = CKSUM_LEN - position;
    if (bytes_remain >= sizeof(uint64_t)) {
        memcpy(p_val, &cksum.data_[position], sizeof(uint64_t));
    }
    else {
        memcpy(p_val, &cksum.data_[position], bytes_remain);
        memcpy(p_val + bytes_remain, &cksum.data_[0], sizeof(uint64_t) - bytes_remain);
    }

    return val;
};

#define ADD_HASH_FUNCTION(start)	\
    inline uint64_t hash_function_ ## start(const Checksum& cksum)	\
    {	\
    	return hash_for_checksum(cksum, start);	\
    }

ADD_HASH_FUNCTION(0);
ADD_HASH_FUNCTION(1);
ADD_HASH_FUNCTION(2);
ADD_HASH_FUNCTION(3);
ADD_HASH_FUNCTION(4);
ADD_HASH_FUNCTION(5);
ADD_HASH_FUNCTION(6);
ADD_HASH_FUNCTION(7);
ADD_HASH_FUNCTION(8);
ADD_HASH_FUNCTION(9);
ADD_HASH_FUNCTION(10);
ADD_HASH_FUNCTION(11);
ADD_HASH_FUNCTION(12);
ADD_HASH_FUNCTION(13);
ADD_HASH_FUNCTION(14);
ADD_HASH_FUNCTION(15);
ADD_HASH_FUNCTION(16);
ADD_HASH_FUNCTION(17);
ADD_HASH_FUNCTION(18);
ADD_HASH_FUNCTION(19);

typedef uint64_t (*BloomFilterFunctionPtr)(const Checksum& cksum);

extern const BloomFilterFunctionPtr kBloomFilterFunctions[];

#endif // _BLOOM_FILTER_FUNCTIONS_H_














