#include "cds_cache.h"
#include <libhashkit/hashkit.h>

LoggerPtr cds_cache_logger(Logger::getLogger("CdsCache"));

uint32_t first_4_bytes(const char *key, size_t key_length, void *context)
{
    uint32_t hash_val;
    if (key_length >= 4) {
        memcpy(&hash_val, key, 4);
        return hash_val;
    }
    else
        return 0;
}

CdsCache::CdsCache()
{
    options_ = kMemcacheOptions;
    Init();
}

CdsCache::CdsCache(const string& mc_options)
{
    options_ = mc_options;
    Init();
}

void CdsCache::Init()
{
    memcached_return_t rc;
    p_memcache_ = memcached(options_.c_str(), options_.size());
    hashkit_st *p_newkit = hashkit_create(NULL);
    if (p_newkit != NULL) {
        hashkit_return_t hash_rc = hashkit_set_custom_function(p_newkit, first_4_bytes, NULL);
        rc = memcached_set_hashkit(p_memcache_, p_newkit);
        if (rc != MEMCACHED_SUCCESS) 
            LOG4CXX_ERROR(cds_cache_logger, "Couldn't set hashkit: %s\n" << memcached_strerror(p_memcache_, rc));
    }
}

CdsCache::~CdsCache()
{
    if (p_memcache_ != NULL)
        memcached_free(p_memcache_);
}




















