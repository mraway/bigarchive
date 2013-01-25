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

/*
const char* CdsIndex::options_ = "--SERVER=128.111.46.222:11211 "
    "--SERVER=128.111.46.96:11211 --SERVER=128.111.46.221:11211 "
    "--SERVER=128.111.46.132:11211 --BINARY-PROTOCOL";
*/
/*
const char* CdsIndex::options_ = "--SERVER=128.111.46.222:11211 "
    "--BINARY-PROTOCOL";
*/

uint32_t first_4_bytes(const char *key, size_t key_length, void *context);

class CdsCache {
public:
    CdsCache(const string& mc_options);

    ~CdsCache();
protected:
    memcached_st* p_memcache_;
};

#endif
