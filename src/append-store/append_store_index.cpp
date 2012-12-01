#include "append_store_index.h"
#include "append_store_exception.h"

using namespace apsara;
using namespace apsara::pangu;
using namespace apsara::AppendStore;

IndexVector::IndexVector(const std::string& fname)
 {
     LoadFromFile(fname);
 }
 
IndexRecord IndexVector::at(uint32_t idx) const
{
     return mValues.at(idx);
}

IndexVector::const_index_iterator IndexVector::find(IndexType key) const
{
    uint32_t pos;
    if (bisearch(&mValues[0], 0, mValues.size(), key, pos)) //key exists
    {
        return begin()+pos;
    }
    else
    {
        return end();
    }
}

uint32_t IndexVector::size() const
{
    return mValues.size();
}

IndexVector::const_index_iterator IndexVector::begin() const
{
    return mValues.begin();
}

IndexVector::const_index_iterator IndexVector::end() const
{
    return mValues.end();
}

void IndexVector::LoadFromFile(const std::string& fname)
{
    // for pangu LogFile: 
    //    the last chunk: the longest chunk in 3 replica,
    //    chunks in the middle:  the shortest chunk in 3 replica.
    //
    // int32_t size = PanguHelper::GetFileSize(fname);
    // if (size) 
    //     condition size!=0 is not correct due to latency
   
    LogFileInputStreamPtr ifsPtr = PanguHelper::OpenLog4Read(fname);

    try
    {
        do
        {
            uint32_t indexSize = ifsPtr->GetNextLogSize();
            if (indexSize != 0)
            {

                std::string buffer;
                buffer.resize(indexSize, 0);
                ifsPtr->ReadLog(&buffer[0], indexSize);
                std::stringstream ss(buffer);
                IndexRecord r;
                r.Deserialize(ss);
                mValues.push_back(r);
            }
            else 
            {
                break;
            }

        } while(true);
        ifsPtr->Close();
    }
    catch (apsara::ExceptionBase& e)
    {
        if (ifsPtr)
        {
            ifsPtr->Close();
        }
        APSARA_THROW(AppendStoreReadException, "Load index file exception " + e.ToString());
    }
}

bool IndexVector::bisearch(const IndexRecord* val_v, uint32_t start, uint32_t nele,
                        IndexType key, uint32_t& pos)
{
    bool found = false;
    int first = start;
    int last  = start + nele  - 1;

    while ( first <= last )
    {
        uint32_t mid = (first+last) >> 1;
        if ((val_v[mid].mIndex>=key) && (mid==0))
        {
            found = true;
            pos = mid;
            break;
        }
        else if ((val_v[mid].mIndex>=key) && (val_v[mid-1].mIndex<key))
        {
            found = true;
            pos = mid;
            break;
        }
        else if (val_v[mid].mIndex < key)
        {
            first = mid + 1;
        }
        else
        {
            last = mid - 1;
        }
    }

    if ( !found ) pos = last + 1;
    return found;
}

