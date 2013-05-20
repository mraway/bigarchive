#include "append_store_index.h"
#include "../include/exception.h"
#include "../include/file_system_helper.h"
#include "../include/file_helper.h"

LoggerPtr IndexVector::logger_ = Logger::getLogger( "IndexVector");

IndexVector::IndexVector(const std::string& fname)
{
	LoadFromFile(fname);
}
 
IndexRecord IndexVector::at(uint32_t idx) const
{
	return mValues.at(idx);
}

void IndexRecord::toBuffer(char *buffer) {
	uint32_t s_offset = sizeof(OffsetType);
	uint32_t s_index = sizeof(IndexType);
	memcpy(buffer, &mOffset, s_offset);
	memcpy((buffer + (s_offset)), &mIndex, s_index);
}

void IndexRecord::fromBuffer(char *buffer) {
	uint32_t s_offset = sizeof(OffsetType);
	uint32_t s_index = sizeof(IndexType);
	memcpy(&mOffset, buffer, s_offset);
	memcpy(&mIndex, (buffer + (s_offset)), s_index);
}


IndexVector::const_index_iterator IndexVector::find(IndexType key) const
{
    uint32_t pos;
    if (bisearch(&mValues[0], 0, mValues.size(), key, pos)) //key exists
    {
        return begin() + pos;
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
    if (!FileSystemHelper::GetInstance()->IsFileExists(fname)) {
        LOG4CXX_ERROR(logger_, "index file " << fname << " not exist");
        return;
    }
  
    long file_size = FileSystemHelper::GetInstance()->GetSize(fname);
    if(file_size <= 0) {
		LOG4CXX_ERROR(logger_, "incorrect index file size: " << file_size);
    	return;
    }

    LOG4CXX_DEBUG(logger_, "reading index file: " << fname << ", size is : " << file_size);

    FileHelper *qfsFH = FileSystemHelper::GetInstance()->CreateFileHelper(fname, O_RDONLY); 
    try
    {
        do
        {
            uint32_t indexSize = qfsFH->GetNextLogSize();
			LOG4CXX_DEBUG(logger_, "Index size from getNextLogSize : " << indexSize);
            if (indexSize != 0)
            {
                char *buffer = new char[indexSize]; // indexSize should be equal to IndexRecord Size !!!
                qfsFH->Read(buffer, indexSize);
                IndexRecord r;
                r.fromBuffer(buffer);
                mValues.push_back(r);
            }
            else 
            {
                break;
            }

        } while(true);
        qfsFH->Close();
        FileSystemHelper::GetInstance()->DestroyFileHelper(qfsFH);
    }
    catch (ExceptionBase& e)
    {
        if (qfsFH)
        {
            qfsFH->Close();
            FileSystemHelper::GetInstance()->DestroyFileHelper(qfsFH);
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

    // pos = pos - 1;

    if ( !found ) pos = last + 1;
    return found;
}

