// see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html

#ifndef _BLOOM_FILTER_FUNCTIONS_H_
#define _BLOOM_FILTER_FUNCTIONS_H_

/*
 * pick the first eight bytes start from func_id as bloom filter hash
 */
inline uint64_t hash_for_uint64(uint64_t ref_id, int start)
{
    uint64_t val = 0;
    return val;
};

#define ADD_HASH_FUNCTION_UINT64(start)	\
    inline uint64_t hash_function_ ## start(uint64_t val)	\
    {	\
    	return hash_for_uint64(val, start);	\
    }

ADD_HASH_FUNCTION_UINT64(0);
ADD_HASH_FUNCTION_UINT64(1);
ADD_HASH_FUNCTION_UINT64(2);
ADD_HASH_FUNCTION_UINT64(3);
ADD_HASH_FUNCTION_UINT64(4);
ADD_HASH_FUNCTION_UINT64(5);
ADD_HASH_FUNCTION_UINT64(6);
ADD_HASH_FUNCTION_UINT64(7);

typedef uint64_t (*BfFuncUint64Ptr)(uint64_t val);

extern const BfFuncUint64Ptr kBfFuncsUint64[];

#endif // _BLOOM_FILTER_FUNCTIONS_H_














