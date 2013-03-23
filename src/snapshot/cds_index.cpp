#include <unordered_map>
#include "cds_index.h"

void CdsIndex::LoadCds(istream &is)
{
    Block blk;
    memcached_return_t rc;
    while (blk.FromStream(is)) {
        rc = memcached_set(p_memcache_, blk.cksum_.data_, CKSUM_LEN, (char*)&blk.offset_, sizeof(uint64_t), (time_t)0, (uint32_t)0);
        if (rc != MEMCACHED_SUCCESS)
            LOG4CXX_ERROR(logger_, "Couldn't set key: " << blk.ToString());
    }
}

bool CdsIndex::Set(Checksum& cksum, uint64_t offset)
{
    memcached_return_t rc;
    rc = memcached_set(p_memcache_, cksum.data_, CKSUM_LEN, (char*)&offset, sizeof(uint64_t), (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS) {
        LOG4CXX_ERROR(logger_, "Couldn't set key: " << memcached_strerror(p_memcache_, rc));
        return false;
    }
    return true;
}

bool CdsIndex::Get(const Checksum& cksum, uint64_t* offset)
{
    memcached_return_t rc;
    size_t len = 0;
    char* value = memcached_get(p_memcache_, cksum.data_, CKSUM_LEN, &len, uint32_t(0), &rc);
    if (rc != MEMCACHED_SUCCESS || len != sizeof(uint64_t)) {
        if (value != NULL) free(value);
        return false;
    }
    memcpy((char*)offset, value, sizeof(uint64_t));
    if (value != NULL) free(value);
    return true;
}

bool CdsIndex::BatchGet(const Checksum* cksums, size_t num_cksums, bool *results, uint64_t *offsets)
{
    size_t *key_length = new size_t[num_cksums];
    for (size_t i = 0; i < num_cksums; ++i)
        key_length[i] = CKSUM_LEN;

    char** p_cksums = new char*[num_cksums];
    for (size_t i = 0; i < num_cksums; ++i)
        p_cksums[i] = (char*)cksums[i].data_;

    memcached_return_t rc = memcached_mget(p_memcache_, p_cksums, key_length, num_cksums);
    if (rc != MEMCACHED_SUCCESS) {
        LOG4CXX_ERROR(logger_, "Multi get failed");
        return false;
    }

    memcached_result_st *p_result;
    unordered_map<string, uint64_t> result_map;
    uint64_t offset;
    while ((p_result = memcached_fetch_result(p_memcache_, NULL, &rc))) {
        if (rc == MEMCACHED_SUCCESS) {
            memcpy(&offset, memcached_result_value(p_result), memcached_result_length(p_result));
            result_map[string(memcached_result_key_value(p_result), memcached_result_key_length(p_result))] = offset;
        }
        memcached_result_free(p_result);
    }

    for (size_t i = 0; i < num_cksums; ++i) {
        unordered_map<string, uint64_t>::iterator it = result_map.find(string(p_cksums[i], CKSUM_LEN));
        if (it == result_map.end())
            results[i] = false;
        else {
            results[i] = true;
            offsets[i] = it->second;
        }
    }

    delete[] p_cksums;
    delete[] key_length;
    return true;
}



















