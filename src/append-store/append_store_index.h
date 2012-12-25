#ifndef _APPENDSTORE_INDEX_H_
#define _APPENDSTORE_INDEX_H_

#include <assert.h>

#include <string>
#include <vector>
#include <algorithm>
#include <memory>
#include <map>
// #include <qfs_file_helper.h>
// #include <qfs_file_system_helper.h>
// #include "apsara/pangu.h"
// #include "pangu_helper.h"
// #include "serialize.h"
#include "append_store_types.h"


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

    OffsetType mOffset;
    IndexType  mIndex;
};

class IndexVector
{
public:
    typedef std::vector<IndexRecord>::const_iterator const_index_iterator;

public:
    IndexVector(const std::string& file);

    ~IndexVector(){};

    const_index_iterator find(IndexType key) const;
 
    uint32_t size() const;

    bool empty() const;

    IndexRecord at(uint32_t idx) const;
    
    const_index_iterator begin() const;

    const_index_iterator end() const;

    void LoadFromFile(const std::string& fname);

private:
    std::vector<IndexRecord> mValues;         // in memory index data

private:
    static bool bisearch(const IndexRecord* val_v, uint32_t start, uint32_t nele,  IndexType key, uint32_t& pos);
};

#endif

