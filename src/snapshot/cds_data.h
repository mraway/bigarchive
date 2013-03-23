/*
 * CDS data access interface
 */

#ifndef _CDS_DATA_H_
#define _CDS_DATA_H_

#include "cds_cache.h"
#include "trace_types.h"
#include "snapshot_types.h"
#include "../include/file_helper.h"

class CdsData : public CdsCache
{
public:
    CdsData(const string& cds_name, const string& mc_options);

    ~CdsData() {};

    /*
     * given a hash, return data if found in memcached
     */
    bool GetFromCache(const Checksum& cksum, char* buf, size_t* len);

    /*
     * add data to memcached
     */
    bool PutToCache(const Checksum& cksum, char* buf, size_t len);

    /*
     * read data from CDS, return the size of read data
     */
    int Read(BlockMeta& bm);

private:
    /*
     * read from file system
     */
    int ReadFromFS(uint64_t offset, char* buf, size_t len);

private:
    FileHelper* p_cdsdata_;
    char buf_[MAX_BLOCK_SIZE];
};

#endif

















