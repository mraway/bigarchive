#include "append_store_types.h"


using namespace apsara::AppendStore;


DataRecord::DataRecord() 
{
}

DataRecord::DataRecord(const std::string& s, const IndexType& i)
    : mVal(s), mIndex(i)
{ 
}

void apsara::AppendStore::DataRecord::Serialize(std::ostream& os) const
{
    apsara::Serialize(mVal, os);
    apsara::Serialize(mIndex, os);
}

apsara::AppendStore::DataRecord::DataRecord* apsara::AppendStore::DataRecord::New()
{
    return new apsara::AppendStore::DataRecord();
}

void apsara::AppendStore::DataRecord::Deserialize(std::istream& is)
{
    apsara::Deserialize(mVal, is);
    apsara::Deserialize(mIndex, is);
}

void apsara::AppendStore::DataRecord::Copy(const Serializable& rec)
{
    const apsara::AppendStore::DataRecord& tmpRec = dynamic_cast<const apsara::AppendStore::DataRecord&>(rec);
    mVal = tmpRec.mVal;
    mIndex = tmpRec.mIndex;
}


CompressedDataRecord::CompressedDataRecord() 
{
}

CompressedDataRecord::CompressedDataRecord(const IndexType& index, const uint32_t& num, 
    const uint32_t& len, const uint32_t& compresslen, const std::string& value)
    : mIndex(index), 
      mRecords(num),
      mOrigLength(len),
      mCompressLength(compresslen),
      mData(value)
{ 
}

void CompressedDataRecord::Serialize(std::ostream& os) const
{
    apsara::Serialize(mIndex, os);
    apsara::Serialize(mRecords, os);
    apsara::Serialize(mOrigLength, os);
    apsara::Serialize(mCompressLength, os);
    // cannot use apsara::Serialize(mData, os);
    apsara::Serialize(mCompressLength, os);
    os.write(&mData[0], mCompressLength);
}

CompressedDataRecord* CompressedDataRecord::New()
{
    return new CompressedDataRecord();
}

void CompressedDataRecord::Deserialize(std::istream& is)
{
    apsara::Deserialize(mIndex, is);
    apsara::Deserialize(mRecords, is);
    apsara::Deserialize(mOrigLength, is);
    apsara::Deserialize(mCompressLength, is);
    apsara::Deserialize(mData, is);
}

void CompressedDataRecord::Copy(const Serializable& rec)
{
    const CompressedDataRecord& tmpRec = dynamic_cast<const CompressedDataRecord&>(rec);
    mIndex          = tmpRec.mIndex;
    mRecords        = tmpRec.mRecords;
    mOrigLength     = tmpRec.mOrigLength;
    mCompressLength = tmpRec.mCompressLength;
    mData           = tmpRec.mData;
}


apsara::AppendStore::DeleteRecord::DeleteRecord(const std::string& src)
{
    if ( src.size()!=sizeof(IndexType))
    {
        mIndex = (IndexType)-1;
        return;
    }
    mIndex = *reinterpret_cast<IndexType*>(const_cast<char*>(&src[0]));
}

DeleteRecord::DeleteRecord(IndexType index)
    : mIndex(index)
{
}

std::string apsara::AppendStore::DeleteRecord::ToString()
{
    std::string ret;
    ret.append(reinterpret_cast<char*>(&mIndex), sizeof(mIndex));
    return ret;
}

bool apsara::AppendStore::DeleteRecord::isValid() const
{
    return (mIndex <=0x0000ffffffffffffllu);
    //return (mIndex != (IndexType)-1);
}

apsara::AppendStore::Handle::Handle(const std::string& src)
{
    if (src.size() != 8)
    {
        mChunkId =(uint16_t)-1;
        mIndex = (uint64_t) -1;
        return;
    }

    uint64_t tmp = *reinterpret_cast<uint64_t*>(const_cast<char*>(&src[0]));
    mChunkId = (tmp>>48) & 0xffff;
    mIndex = tmp & 0x0000ffffffffffffllu;
}

Handle::Handle(ChunkIDType id, IndexType index)
    : mChunkId(id), mIndex(index)
{
}

Handle::Handle(const Handle& other) 
{
    mChunkId = other.mChunkId;
    mIndex   = other.mIndex;
}

std::string apsara::AppendStore::Handle::ToString()
{
    std::string ret;
    uint64_t tmp = ((0xffffffffff & mChunkId) << 48) | mIndex;
    ret.append(reinterpret_cast<char*>(&tmp), sizeof(tmp));
    return ret;
}

bool apsara::AppendStore::Handle::isValid() const
{
    return (mChunkId != (uint16_t)-1) && (mIndex != (uint64_t)-1);
}

bool apsara::AppendStore::Handle::operator==(const Handle& other) const
{
    return (mChunkId == other.mChunkId && mIndex == other.mIndex);
}

bool apsara::AppendStore::Handle::operator!=(const Handle& other) const
{
    return !(*this == other);
}

bool apsara::AppendStore::Handle::operator<(const Handle& other) const
{
    if (mChunkId < other.mChunkId)
    {
        return true; 
    }
    else if (mChunkId == other.mChunkId) 
    {
        return mIndex < other.mIndex;
    }
    return false;
}

Handle& Handle::operator=(const Handle& other)
{
    if (this != &other) 
    {
        mChunkId = other.mChunkId;
        mIndex   = other.mIndex;
    }
    return *this;
}


StoreMetaData::StoreMetaData(uint32_t omajor, uint32_t ominor, uint64_t chunksize, 
    uint32_t indexinterval, DataFileCompressionFlag compresstype)
    : storemajor(omajor),
      storeminor(ominor),
      maxChunkSize(chunksize),
      blockIndexInterval(indexinterval),
      compressionFlag(compresstype)
{
}

bool StoreMetaData::check(uint32_t omajor, uint32_t ominor) const
{
    if (omajor==storemajor && ominor==storeminor)
    {
        return true;
    }
    return false;
}

bool Cache::Find(const Handle& idx, std::string* data) const
{
    Cache::CacheMapType::const_iterator it = mCacheMap.find(idx);
    if (it == mCacheMap.end())
    {
        return false;
    }
    data->clear();
    data->append(it->second);
    return true;
}

uint32_t Cache::Size() const
{
    return mCacheMap.size();
}

uint32_t Cache::GetTotalSize() const
{
    uint32_t size =0;
    for ( Cache::CacheMapType::const_iterator it = mCacheMap.begin(); it!=mCacheMap.end(); ++it)
    {
        size += it->second.size();
    }
    return size;
}

void Cache::Clear() 
{
    mCacheMap.clear();
}

void Cache::Insert(const Handle& idx, const std::string& data)
{
    mCacheMap.insert(std::make_pair(idx, data));
}

void Cache::Remove(const Handle& idx)
{
    mCacheMap.erase(idx);
}

