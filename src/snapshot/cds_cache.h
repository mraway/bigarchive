/*
 * base class for operating cds in memcached
 */
#ifndef _CDS_CACHE_H_
#define _CDS_CACHE_H_

#include <libmemcached/memcached.h>
#include <string>
#include <string.h>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;


extern const char* kMemcacheOptions;

uint32_t first_4_bytes(const char *key, size_t key_length, void *context);

class CdsCache {
public:
    CdsCache();
    CdsCache(const string& mc_options);
    ~CdsCache();

protected:
    void Init();
    string options_;
    memcached_st* p_memcache_;
};

#endif
