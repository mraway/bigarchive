#include "bloom_filter_functions.h"

const BloomFilterFunctionPtr kBloomFilterFunctions[CKSUM_LEN] = {
    &hash_function_0, &hash_function_3, &hash_function_6, &hash_function_9, &hash_function_12, &hash_function_15, &hash_function_18,
    &hash_function_1, &hash_function_4, &hash_function_7, &hash_function_10, &hash_function_13, &hash_function_16, &hash_function_19,
    &hash_function_2, &hash_function_5, &hash_function_8, &hash_function_11, &hash_function_14, &hash_function_17 };
