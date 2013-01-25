/*
 * CDS data in memcached cache
 */

#ifndef _CDS_DATA_CACHE_H_
#define _CDS_DATA_CACHE_H_

#include "cds_cache.h"
#include "dedup_types.h"

class CdsDataCache : public CdsCache
{
public:
    CdsDataCache(const string& mc_options) : CdsCache(mc_options) {};

    ~CdsDataCache() {};

    /*
     * given a hash, return data if found in memcached
     */
    bool Get(Checksum cksum, char* buf, size_t* len);

    /*
     * add data to memcached
     */
    bool Set(Checksum cksum, char* buf, size_t len);
};

#endif










