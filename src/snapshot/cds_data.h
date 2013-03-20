/*
 * CDS data access interface
 */

#ifndef _CDS_DATA_H_
#define _CDS_DATA_H_

#include "cds_cache.h"
#include "trace_types.h"

class CdsData : public CdsCache
{
public:
    CdsData(const string& mc_options) : CdsCache(mc_options) {};

    ~CdsData() {};

    /*
     * given a hash, return data if found in memcached
     */
    bool Get(Checksum& cksum, char* buf, size_t* len);

    /*
     * add data to memcached
     */
    bool Set(Checksum& cksum, char* buf, size_t len);
};

#endif










