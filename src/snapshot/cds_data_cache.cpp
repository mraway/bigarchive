#include "cds_data_cache.h"
#include "cds_index.h"

LoggerPtr cds_data_logger(Logger::getLogger("CdsData"));

bool CdsDataCache::Get(Checksum& cksum, char *buf, size_t* len)
{
    memcached_return_t rc;
    char* value = memcached_get(p_memcache_, cksum.data_, CKSUM_LEN, len, uint32_t(0), &rc);
    if (rc != MEMCACHED_SUCCESS) {
        if (value != NULL) free(value);
        return false;
    }
    if (*len == 0) {
        LOG4CXX_ERROR(cds_data_logger, "Cache returned zero length data");
        return false;
    }
    memcpy(buf, value, *len);
    if (value != NULL) free(value);
    return true;
}

bool CdsDataCache::Set(Checksum& cksum, char *buf, size_t len)
{
    memcached_return_t rc;
    rc = memcached_set(p_memcache_, cksum.data_, CKSUM_LEN, buf, len, (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS) {
        LOG4CXX_ERROR(cds_data_logger, "Couldn't set key: " << memcached_strerror(p_memcache_, rc));
        return false;
    }
    return true;
}



















