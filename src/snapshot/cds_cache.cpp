#include "cds_cache.h"
#include <libhashkit/hashkit.h>

const char* kCdsIndexOptions = 
    "--SERVER=128.111.46.222:11211 "
    "--SERVER=128.111.46.96:11211 "
    "--SERVER=128.111.46.132:11211 "
    "--SERVER=128.111.46.216:11211 "
    "--SERVER=128.111.46.85:11211 "
    "--SERVER=128.111.46.94:11211 "
    "--BINARY-PROTOCOL";
const char* kCdsDataOptions = 
    "--SERVER=128.111.46.222:11985 "
    "--SERVER=128.111.46.96:11985 "
    "--SERVER=128.111.46.132:11985 "
    "--SERVER=128.111.46.216:11985 "
    "--SERVER=128.111.46.85:11985 "
    "--SERVER=128.111.46.94:11985 "
    "--BINARY-PROTOCOL";

/*
const char* kMemcacheOptions = "--SERVER=128.111.46.222:11211 "
    "--BINARY-PROTOCOL";
*/

LoggerPtr CdsCache::logger_ = Logger::getLogger("BigArchive.CDS");

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
    options_ = kCdsIndexOptions;
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
        // hashkit_return_t hash_rc = hashkit_set_custom_function(p_newkit, first_4_bytes, NULL);
        hashkit_set_custom_function(p_newkit, first_4_bytes, NULL);
        rc = memcached_set_hashkit(p_memcache_, p_newkit);
        if (rc != MEMCACHED_SUCCESS) 
            LOG4CXX_ERROR(logger_, "Couldn't set hashkit: %s\n" << memcached_strerror(p_memcache_, rc));
    }
}

CdsCache::~CdsCache()
{
    if (p_memcache_ != NULL)
        memcached_free(p_memcache_);
}




















