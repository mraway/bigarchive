#include "append_store_index.h"
#include "exception.h"
#include "qfs_file_helper.h"

#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;


LoggerPtr iv_logger(Logger::getLogger( "appendstore.chunk_impl"));


IndexVector::IndexVector(const std::string& fname)
{
	DOMConfigurator::configure("/home/prakash/log_config.xml");
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


IndexVector::const_index_iterator IndexVector::find(IndexType key, bool &is_begin) const
{
    uint32_t pos;
    if (bisearch(&mValues[0], 0, mValues.size(), key, pos)) //key exists
    {
	is_begin = false;
	if(pos == 0) {
	 is_begin = true;
	 return begin();
	}
        return begin() + pos - 1;
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
  
    LOG4CXX_DEBUG(iv_logger, "reading index from file : " << fname);
 
    QFSHelper *qfsHelper = new QFSHelper();
    qfsHelper->Connect();//"host", 30000);
    
    int file_size = qfsHelper->getSize(fname);

    LOG4CXX_DEBUG(iv_logger, "index file size is : " << file_size);

    if(file_size <= 0) {
		LOG4CXX_INFO(iv_logger, "not reading index file, because size is : " << file_size);
    	return;
    }

    QFSFileHelper *qfsFH = new QFSFileHelper(qfsHelper, fname, O_RDONLY); 

    try
    {
        do
        {
            uint32_t indexSize = qfsFH->GetNextLogSize();
			LOG4CXX_DEBUG(iv_logger, "Index size from getNextLogSize : " << indexSize);
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
    }
    catch (ExceptionBase& e)
    {
        if (qfsFH)
        {
            qfsFH->Close();
        }
        THROW_EXCEPTION(AppendStoreReadException, "Load index file exception " + e.ToString());
    }
    qfsHelper->DisConnect();
    LOG4CXX_INFO(iv_logger,"IndexVector::LoadedfromFile " << fname);
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

