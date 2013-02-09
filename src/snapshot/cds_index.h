/*
 * CDS index in memcached
 */

#ifndef _CDS_INDEX_H_
#define _CDS_INDEX_H_

#include <iostream>
#include "cds_cache.h"
#include "dedup_types.h"

class CdsIndex : public CdsCache
{
public:
    CdsIndex(const string& mc_options) : CdsCache(mc_options) {};

    ~CdsIndex() {};

    /*
     * load cds index into memcached
     */
    void LoadCds(istream& is);

    /*
     * set one cds index entry
     */
    bool Set(Checksum& cksum, uint64_t offset);

    /*
     * query cds index by block hash
     * return true on found, false on not found
     * if found, the offset of that block in CDS data file is stored in offset
     */
    bool Get(Checksum& cksum, uint64_t* offset);

    /*
     * query cds index by multiple keys
     */
    bool BatchGet(const char * const *p_cksums, size_t num_cksums, bool *results, uint64_t *offsets);
};

#endif
