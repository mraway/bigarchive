#include "cds_data.h"
#include "cds_index.h"

LoggerPtr cds_data_logger(Logger::getLogger("CdsData"));

CdsData::CdsData(const string& cds_name, const string& mc_options) : CdsCache(mc_options)
{
    string cds_datafile = "/cds/" + cds_name;
    p_cdsdata_ = FileSystemHelper::GetInstance()->CreateFileHelper(cds_datafile, O_RDONLY);
    p_cdsdata_->Open();
}

bool CdsData::GetFromCache(const Checksum& cksum, char *buf, size_t* len)
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

bool CdsData::PutToCache(const Checksum& cksum, char *buf, size_t len)
{
    memcached_return_t rc;
    rc = memcached_set(p_memcache_, cksum.data_, CKSUM_LEN, buf, len, (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS) {
        LOG4CXX_ERROR(cds_data_logger, "Couldn't set key: " << memcached_strerror(p_memcache_, rc));
        return false;
    }
    return true;
}

int CdsData::ReadFromFS(uint64_t offset, char* buf, size_t len)
{
    p_cdsdata_->Seek(offset);
    return p_cdsdata_->Read(buf, len);
}

int CdsData::Read(BlockMeta& bm)
{
    size_t s;
    // first try to get data from memcache, key is the checksum
    if (GetFromCache(bm.cksum_, buf_, &s)) {
        if (s == bm.size_) {
            string tmp(buf_, s);
            bm.DeserializeData(tmp);
            return s;
        }
        else
            LOG4CXX_ERROR(logger_, "Read " << s << "from memcache, expect " << bm.size_);
    }
    // then try to read data from CDS data file, here handle refers to the offset
    s = ReadFromFS(bm.handle_, buf_, bm.size_);
    if (s == bm.size_)
        PutToCache(bm.cksum_, buf_, bm.size_);
    else
        LOG4CXX_ERROR(logger_, "Read " << s << "from FS, expect " << bm.size_);

    string tmp(buf_, s);
    bm.DeserializeData(tmp);
    return s;
}


















