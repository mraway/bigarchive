#ifndef  _APPENDSTORE_TYPES_H
#define  _APPENDSTORE_TYPES_H

#include <string>
#include "serialize.h"
#include "store.h" 
#include "stdio.h"
// #include "apsara/common/logging.h" 
// #include "apsara/pangu.h"

typedef uint16_t ChunkIDType; // chunk id
typedef uint64_t IndexType;   // logical index of the data item (the index is location-independent)
typedef uint64_t OffsetType;  // Offset of the data item 


//max number of appends before flush
const uint32_t DF_MAX_PENDING =  100;
const uint32_t DF_CHUNK_SZ = (1024 * 1024 * 1024); //1G
const uint32_t DF_MAX_BLOCK_SZ = (1024 * 1024 * 4); //4M

const int DF_MINCOPY = 3;
const int DF_MAXCOPY = 3;
const int DF_MAXFILENO = 255*255;

//CHKIT
const char AppName[] = "app_name"; // LOCALFILE_APPNAME; 
const char PartName[] = "part_name"; // LOCALFILE_PARTNAME; 

const char* const MetaFileName = ".meta_";

const uint16_t MAJOR_VER = 1;
const uint16_t MINOR_VER = 0;


/*
* data record in data/dat.0
*/
class DataRecord : public marshall::Serializable
{
public:
    DataRecord();

    DataRecord(const std::string& s, const IndexType& i);

    void Serialize(std::ostream& os) const;

    DataRecord* New();

    void Deserialize(std::istream& is);

    void Copy(const Serializable& rec);

    std::string mVal;
    IndexType   mIndex;
};


/*
* compressed record which consists of many data record in data/dat.0
*/
class CompressedDataRecord : public marshall::Serializable
{
public:
    CompressedDataRecord();

    CompressedDataRecord(const IndexType&, const uint32_t&, const uint32_t&, 
                const uint32_t&, const std::string&);

    void Serialize(std::ostream& os) const;

    CompressedDataRecord* New();

    void Deserialize(std::istream& is);

    void Copy(const Serializable& rec);

    IndexType   mIndex;
    uint32_t    mRecords;       // the number of records in compressed block 
    uint32_t    mOrigLength;
    uint32_t    mCompressLength;  
    std::string mData;
                                // ? compression flag     yes/no
};


struct DeleteRecord
{
    IndexType mIndex;

    DeleteRecord(const std::string& src);

    DeleteRecord(IndexType index = (IndexType)-1);

    std::string ToString();

    bool isValid() const;
};


class Handle
{
public:
    ChunkIDType  mChunkId;
    IndexType  mIndex;

    explicit Handle(const std::string& src);

    explicit Handle(ChunkIDType id = (uint16_t)-1, IndexType index = (uint64_t)-1);

    Handle(const Handle&);  
   
    Handle& operator=(const Handle&);

    /* transform Handle into string;
       not for printing;
    */
    std::string ToString();

    bool isValid() const;

    bool operator==(const Handle& other) const;

    bool operator!=(const Handle& other) const;

    bool operator<(const Handle& other) const;
};

struct StoreMetaData
{
    StoreMetaData() : compressionFlag(COMPRESSOR_LZO) {};

    StoreMetaData(uint32_t, uint32_t, uint64_t, uint32_t, DataFileCompressionFlag);

    void fromBuffer(char *buffer);

    void toBuffer(char *buffer);

    bool check(uint32_t, uint32_t) const;

    uint32_t    storemajor;
    uint32_t    storeminor;
    uint64_t    maxChunkSize;
    uint32_t    blockIndexInterval;
    DataFileCompressionFlag  compressionFlag;
};

// Todo: improve
class Cache
{
public:
    bool Find(const Handle& handle, std::string* data) const;

    void Insert(const Handle& handle, const std::string& data);

    void Remove(const Handle& handle);

    void Clear();

    uint32_t Size() const;

    uint32_t GetTotalSize() const;

private:
    typedef std::map<Handle, std::string> CacheMapType;
    CacheMapType mCacheMap;
};

typedef std::tr1::weak_ptr<Cache>   CacheWeakPtr;
typedef std::tr1::shared_ptr<Cache> CachePtr;

#endif//_APPENDSTORE_TYPES_H
