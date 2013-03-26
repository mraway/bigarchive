#ifndef _APPENDSTORE_INDEX_H_
#define _APPENDSTORE_INDEX_H_

#include <assert.h>

#include <string>
#include <vector>
#include <algorithm>
#include <memory>
#include <map>
#include "append_store_types.h"
#include <log4cxx/logger.h>

using namespace log4cxx;

class IndexRecord : public marshall::Serializable
{
public:
    IndexRecord() { };

    IndexRecord(const OffsetType& s, const IndexType& i)
        : mOffset(s), mIndex(i)
        {};

    void Serialize(std::ostream& os) const
    {
        marshall::Serialize(mOffset, os);
        marshall::Serialize(mIndex, os);
    }

    IndexRecord* New()
    {
        return new IndexRecord();
    }

    void Deserialize(std::istream& is)
    {
        marshall::Deserialize(mOffset, is);
        marshall::Deserialize(mIndex, is);
    }

    void Copy(const Serializable& rec)
    {
        const IndexRecord& tmpRec = dynamic_cast<const IndexRecord&>(rec);
        mOffset = tmpRec.mOffset;
        mIndex = tmpRec.mIndex;
    }

    uint32_t Size() const 
    {
        return sizeof(mOffset)+sizeof(mIndex);
    }

    void fromBuffer(char *buffer);
    void toBuffer(char  *buffer); 

public:
    OffsetType mOffset;	// the starting position in data file
    IndexType  mIndex;	// the numberic index, store in the last 6 bytes of handle
};

class IndexVector
{
public:
    typedef std::vector<IndexRecord>::const_iterator const_index_iterator;

public:
    // initialize from a chunk index file
    IndexVector(const std::string& file);

    ~IndexVector(){};

    // search the index vector to locate an entry that covers the given key
    const_index_iterator find(IndexType key) const;
 
    uint32_t size() const;

    bool empty() const;

    IndexRecord at(uint32_t idx) const;
    
    const_index_iterator begin() const;

    const_index_iterator end() const;

    void LoadFromFile(const std::string& fname);

private:
    std::vector<IndexRecord> mValues;         // in memory index data
    LoggerPtr logger_;

private:
    // Wei: don't understand why Xiaogang did this, he should use STL instead
    // search the index vector to find an entry, whose range covers the given index value
    // @param: val_v is the begin position of index vector
    // @param: start is the start search position
    // @param: nele is the length of search range
    // @param: key is the search key (index value)
    // @param: pos is the position of index vector entry if found
    // @retrun: success or fail
    static bool bisearch(const IndexRecord* val_v, uint32_t start, uint32_t nele,  IndexType key, uint32_t& pos);
};

#endif

