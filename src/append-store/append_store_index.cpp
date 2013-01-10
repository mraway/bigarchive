#include "append_store_index.h"
#include "exception.h"
#include "qfs_file_helper.h"

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
  
    // CHKIT
 
    QFSHelper *qfsHelper = new QFSHelper();
    qfsHelper->Connect();//"host", 30000);
    
    QFSFileHelper *qfsFH = new QFSFileHelper(qfsHelper, fname, O_RDONLY); 

    try
    {
        do
        {
            uint32_t indexSize = qfsFH->GetNextLogSize();
            if (indexSize != 0)
            {

                std::string buffer;
                buffer.resize(indexSize, 0);
                qfsFH->Read(&buffer[0], indexSize);
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
        qfsFH->Close();
    }
    catch (ExceptionBase& e)
    {
        if (qfsFH)
        {
            qfsFH->Close();
        }
        THROW_EXCEPTION(AppendStoreReadException, "Load index file exception " + e.ToString());
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

