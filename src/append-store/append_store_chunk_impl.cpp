#include "append_store_chunk.h"
#include "../include/exception.h"

LoggerPtr Chunk::logger_ = Logger::getLogger("Appendstore_chunk");

const char* Defaults::IDX_DIR = "index/";
const char* Defaults::DAT_DIR = "data/";
const char* Defaults::LOG_DIR = "log/";

Chunk::Chunk(const std::string& root, ChunkIDType chunk_id, 
             uint64_t max_chunk_sz, bool append_flag, CompressionCodecWeakPtr weakptr, 
             CacheWeakPtr cacheptr, uint32_t index_interval)
    : mRoot(root), 
      mChunkId(chunk_id), 
      mIndexFileName(GetIdxFname(root, chunk_id)), 
      mLogFileName(GetIdxLogFname(root, chunk_id)), 
      mDataFileName(GetDatFname(root, chunk_id)), 
      mMaxIndex(0), 
      mLastData(0), 
      mMaxChunkSize(max_chunk_sz), 
      mFlushCount(0), 
      mDirty(append_flag), 
      mChunkCodec(weakptr),
      mCachePtr(cacheptr),
      mBlockIndexInterval(index_interval)
{
    mFileSystemHelper = FileSystemHelper::GetInstance(); 

    mDataInputFH = NULL;
    mDataOutputFH = NULL;
    mIndexOutputFH = NULL;
    mDeleteLogFH = NULL;
    
    CheckIfNew();
    LoadIndex();
    LoadData(append_flag);
}

Chunk::Chunk(const std::string& root, ChunkIDType chunk_id)
    : mRoot(root), 
      mChunkId(chunk_id), 
      mLogFileName(GetIdxLogFname(root, chunk_id)), 
      mDirty(false)
{
	mFileSystemHelper = FileSystemHelper::GetInstance();
	LoadDeleteLog();
}

void Chunk::LoadDeleteLog()
{
	mDeleteLogFH = FileSystemHelper::GetInstance()->CreateFileHelper(mLogFileName, O_APPEND);
	if(mFileSystemHelper->IsFileExists(mLogFileName) == false) {
		mDeleteLogFH->Create();
	}
	mDeleteLogFH->Open();
	LOG4CXX_INFO(logger_, "Chunk::LoadDeleteLog() Completed");
}

void Chunk::CheckIfNew()
{
    bool dexist;
    bool iexist;
    try
    {
        dexist = mFileSystemHelper->IsFileExists(mDataFileName); 
        iexist = mFileSystemHelper->IsFileExists(mIndexFileName);
    }
    catch(ExceptionBase& e)
    {
        LOG4CXX_ERROR(logger_, "Error in CheckIfNew: " << e.ToString());
        throw;
    }
    if (! ((dexist && iexist) || (!dexist && !iexist)) )
    {
        LOG4CXX_ERROR(logger_, "mDataFileName and mIndexFileName not co-exist");
        THROW_EXCEPTION(ExceptionBase, "mDataFileName and mIndexFileName not co-exist");
    }

    if (!dexist && !iexist)
    {
    	FileHelper* fh = mFileSystemHelper->CreateFileHelper(mDataFileName, O_WRONLY); 
		fh->Create();
        mFileSystemHelper->DestroyFileHelper(fh);

       	fh = mFileSystemHelper->CreateFileHelper(mIndexFileName, O_WRONLY); 
        fh->Create();
        mFileSystemHelper->DestroyFileHelper(fh);
    }
	LOG4CXX_INFO(logger_, "Chunk::CheckIfNew() Completed");
}
   
Chunk::~Chunk()
{
    Flush();
    Close();
}

void Chunk::Flush()
{
    if (mDirty)
    {
        if (mFlushCount > 0)
        {
            AppendIndex();
        }
    }
}

IndexType Chunk::Append(const std::string& data)
{
    if (IsChunkFull() == true)
    {
        std::stringstream ss;
        ss << "Chunk is full but still used somehow ";
        THROW_EXCEPTION(AppendStoreWriteException, ss.str());
    }

    /*
      if (mBlockStream.str().size()+data.size() >= DF_MAX_BLOCK_SZ)
      {
      AppendIndex();
      }
    */

    IndexType new_index = ++mMaxIndex; // GenerateIndex();
    DataRecord r(data, new_index);
    r.Serialize(mBlockStream);
    mFlushCount++;

    if (mFlushCount >= mBlockIndexInterval)
    {
        AppendIndex();
    }

    //LOG4CXX_INFO(logger_, "Chunk::Append Completed");
    return new_index;
}

void Chunk::AppendIndex()
{
    if (mFlushCount == 0)
    {
        return;
    }

    OffsetType written = AppendRaw(mMaxIndex, mFlushCount, mBlockStream.str());
    mLastData = written;
    mFlushCount = 0;

    mBlockStream.str("");
    mBlockStream.clear();         //clear stringstream 

    IndexRecord r;
    r.mOffset = mLastData;
    r.mIndex = mMaxIndex;

    char *buffer = new char[r.Size()];

    r.toBuffer(buffer);


    int32_t retryCount = 0;
    do
    {
        try
        {
            int fos = mIndexOutputFH->Write(buffer, r.Size());
            //LOG4CXX_DEBUG(logger_, "index flushed : data wrote -- " << buffer);
            LOG4CXX_DEBUG(logger_, "index flushed : data size --- " << r.Size());
            LOG4CXX_DEBUG(logger_, "index flushed : index file size (size + data) -- " << fos);
            LOG4CXX_DEBUG(logger_, "index file size : getSize " << mFileSystemHelper->GetSize(mIndexFileName));
            LOG4CXX_WARN(logger_, "Index Flushed into " << mIndexFileName << ":" << r.mIndex << ":" << r.mOffset);
            break;
        }
        catch(ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger_, "IndexOutputStream corrupt << e.ToString()");
            try
            {
                mIndexOutputFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG4CXX_ERROR(logger_, "Failed close file after write fail " << e.ToString());
            }

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }

            mIndexOutputFH = mFileSystemHelper->CreateFileHelper(mIndexFileName, O_APPEND); 
            mIndexOutputFH->Open();

            if (retryCount > 1)
            {
                LOG4CXX_ERROR(logger_, "DataOutputStream FlushLog fail after retry " << e.ToString());
                throw;
            }
        }
    } while (retryCount <= 1);
    LOG4CXX_INFO(logger_, "Chunk::AppendIndex Completed");
    // cout << endl << "Time Chunk::AppendIndex : " << t.stop() << " ms";
};
       

bool Chunk::Read(IndexType index, std::string* data) 
{
    IndexVector::const_index_iterator it;
    if (!IsValid(index) || (it=mIndexMap->find(index)) == mIndexMap->end())
    {
        return false;
    }
    if (IsValid(it->mIndex) == false)
    {
        return false;
    }
    

    OffsetType startOffset = it->mOffset;

    std::string buf;
    bool ret = ReadRaw(startOffset, buf);
    ret = ret && ExtractDataFromBlock(buf, index, data);
    LOG4CXX_INFO(logger_, "Chunk::Read Completed");
    return ret;
}

bool Chunk::ExtractDataFromBlock(const std::string& buf, IndexType index, std::string* data)
{
    CachePtr cachesharedptr = mCachePtr.lock();
    if (cachesharedptr == NULL)
    {
        LOG4CXX_ERROR(logger_, "the cache has been destructed.");
        THROW_EXCEPTION(AppendStoreReadException, "Failed to get cachePtr");
    }

    bool ret = false;
    cachesharedptr->Clear();
    std::stringstream streamBuf(buf);
    do
    {
        if (streamBuf.peek() == EOF)
            break;

        // Fixme: bad  throw
        if (streamBuf.eof() || streamBuf.bad())
            break;

        // Fixme: exception
        DataRecord r;
        r.Deserialize(streamBuf);
        if (!IsValid(r.mIndex))
        {
            THROW_EXCEPTION(AppendStoreReadException, "Failed extracting data from block");
        }

        if (r.mIndex == index)
        {
            data->clear();  
            data->append(r.mVal);
            ret = true;
        }
        cachesharedptr->Insert(Handle(mChunkId, r.mIndex), r.mVal);
    } while(true);
    LOG4CXX_INFO(logger_, "Chunk::ExtractDataFromBlock Completed");
    return ret;
}

bool Chunk::Remove(const IndexType& index)
{
    DeleteRecord d(index);
    std::string tmp = d.ToString(); 

    // may retry once 
    int32_t retryCount = 0;
    do
    {
        try
        {
        	mDeleteLogFH->Flush(&tmp[0], tmp.size());
            break;
        }
        // CHKIT - this exceptions is not throwed from our write method !!! 
        // this will be never called	
        catch(ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger_, "DeleteLogStream corrupt " << e.ToString());
            try
            {
            	mDeleteLogFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG4CXX_ERROR(logger_, "Failed close file after write fail " << e.ToString());
            }
            mDeleteLogFH->Seek(0);

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }
        
            mDeleteLogFH = mFileSystemHelper->CreateFileHelper(mLogFileName, O_APPEND); // WRITE);

            if (retryCount > 1)
            {
                LOG4CXX_ERROR(logger_, "DataOutputStream FlushLog fail after retry " << e.ToString());
                throw;
            }
        }
    } while (retryCount <= 1);

    return true;
}

bool Chunk::LoadIndex()
{
    mIndexMap.reset(new IndexVector(mIndexFileName));
    uint32_t size = mIndexMap->size();
    if (size > 0)
    {
        mMaxIndex = mIndexMap->at(size-1).mIndex;
    }
    return true;
}

bool Chunk::LoadData(bool flag)
{
    if (flag)
    {
        try
        {
            mLastData = mFileSystemHelper->GetSize(mDataFileName);
        }
        catch(ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger_, "error on get data file size" << e.ToString());
            throw;
        }

        mDataOutputFH = mFileSystemHelper->CreateFileHelper(mDataFileName, O_APPEND); // O_WRONLY);// WRITE);
        mDataOutputFH->Open();
        mIndexOutputFH = mFileSystemHelper->CreateFileHelper(mIndexFileName, O_APPEND); // O_WRONLY); //WRITE);
        mIndexOutputFH->Open();
    }
    else 
    {
    	mDataInputFH = mFileSystemHelper->CreateFileHelper(mDataFileName, O_RDONLY); // READ);
    	mDataInputFH->Open();
    }
    return true;
}

uint32_t Chunk::GetChunkSize(const std::string& root, ChunkIDType chunk_id)
{
    std::string dat_file = GetDatFname(root, chunk_id);
    return FileSystemHelper::GetInstance()->GetSize(dat_file);
}

uint16_t Chunk::GetMaxChunkID(const std::string& root)
{
    std::string data_root = root + Defaults::DAT_DIR;
    std::vector<std::string> data_files; 
    try
    {
    	// CHECK IT
        FileSystemHelper::GetInstance()->ListDir(data_root, data_files);
    }
    catch(ExceptionBase& e)
    {
        LOG4CXX_ERROR(logger_, "Error " << e.ToString());
        throw;
    }

    // find the largest chunk ID.
    uint32_t chunk_id = 0;
    std::vector<std::string>::const_reverse_iterator it     = data_files.rbegin();
    std::vector<std::string>::const_reverse_iterator it_end = data_files.rend();
    for (; it != it_end; ++ it)
    {
        if (it->find("dat")!=0)
        {
            continue;
        }
        std::string::size_type pos = it->find_last_of('.');
        if (pos != std::string::npos)
        {
            std::string ext = it->substr(pos+1);
            uint32_t tmp_id = strtol(ext.c_str(), 0, 16);
            if (tmp_id > chunk_id) chunk_id = tmp_id;
        }
    }
    return chunk_id;
}

bool Chunk::IsChunkFull() const
{
    uint64_t size = (mLastData& 0xFFFFFFFF)+((mLastData >> 32) & 0xFFFFFFFF)*64*1024*1024;
    return ((size >= mMaxChunkSize)||(mMaxIndex==(IndexType)-1));
}

inline IndexType Chunk::GenerateIndex()
{
    if (mMaxIndex != IndexType(-1)) 
    {
        ++mMaxIndex;
        return mMaxIndex;
    }
    // CHKIT
    THROW_EXCEPTION(AppendStoreInvalidIndexException, "index overflow during GenerateIndex()");

}

bool Chunk::IsValid(const IndexType& value)
{
    return ((value&0xc000000000000000llu)==0);
}

std::string Chunk::GetIdxFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%sidx.%x",root.c_str(), Defaults::IDX_DIR, chunk_id);
    return buf;
}

std::string Chunk::GetIdxLogFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%slog.%x",root.c_str(), Defaults::LOG_DIR, chunk_id);
    return buf;
}

std::string Chunk::GetDatFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%sdat.%x",root.c_str(), Defaults::DAT_DIR, chunk_id);
    return buf;
}

std::string Chunk::GetLogFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%slog.%x",root.c_str(), Defaults::LOG_DIR, chunk_id);
    return buf;
}


bool Chunk::Close()
{

    if (mDataInputFH != NULL) {
        try
        {
            mDataInputFH->Close();
            mDataInputFH = NULL;
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger_, "Failed to close input data file" << ex.ToString());
        }
    }

    if (mDataOutputFH != NULL)
    {
        try
        {
            mDataOutputFH->Close();
            mDataOutputFH = NULL;
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger_, "Failed to close output data file " << ex.ToString());
        }
        // mDataOutputFH->Seek(0); // .reset();
    }

    if (mIndexOutputFH != NULL)
    {
        try
        {
            mIndexOutputFH->Close();
            mIndexOutputFH = NULL;
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger_, "Failed to close output index file " << ex.ToString());
        }
        // mIndexOutputFH->Seek(0); //reset();
    }

    if (mDeleteLogFH != NULL)
    {
        try
        {
            mDeleteLogFH->Close();
            mDeleteLogFH = NULL;
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger_, "Failed to close deletelog file" <<  ex.ToString());
        }
        // mDeleteLogFH->Seek(0); // .reset();
    }

    LOG4CXX_INFO(logger_, "Closed Current Chunk and its file helpers");
    return true;
}


//Karim
//Reads the chunk at Offset and the REAL data excluding the header and stored it in the variable data
bool Chunk::ReadRaw(const OffsetType& offset, std::string& data) 
{
    try
    {
        mDataInputFH->Seek(offset);//Karim: goes to the given offset in the chunk data file
        uint32_t read_len = mDataInputFH->GetNextLogSize();//Karim: get the data length from the dataInputFH=>put it in read_len
        //Karim: Also remember that GetNextLogSize reads the whole header so the file offset now is at the beginning of the real data (chunk date)
        std::string blkdata;
        blkdata.resize(read_len, 0);
        uint32_t size = read_len;
        // CHKIT
        mDataInputFH->Read(&blkdata[0], read_len);

        if (read_len != size)
        {
            std::stringstream ss;
            ss<<"DataInputStream file read error, need size: " << size <<" actual size: "<<read_len;
            // CHKIT
            THROW_EXCEPTION(AppendStoreReadException, ss.str());
        }

        CompressedDataRecord crd;
        std::stringstream    sstream(blkdata);
        crd.Deserialize(sstream);

        // decompress 
        CompressionCodecPtr sharedptr = mChunkCodec.lock();
        if (sharedptr == NULL)
        {
            LOG4CXX_ERROR(logger_, "Error : the compression codec has been destructed.");
            THROW_EXCEPTION(AppendStoreCompressionException, "decompression error inside ReadRaw()");
        }

        uint32_t uncompressedSize;
        data.resize(crd.mOrigLength);
        int retc = sharedptr->decompress(&(crd.mData[0]), crd.mCompressLength, &data[0], uncompressedSize);
        if (uncompressedSize != crd.mOrigLength)
        {
            LOG4CXX_ERROR(logger_, ("Error : error when decompressing due to invalid length"));
            THROW_EXCEPTION(AppendStoreCompressionException, "decompression invalid length");
        }
        if (retc < 0)
        {
            LOG4CXX_ERROR(logger_, ("Error : decompression codec error when decompressing inside ReadRaw()"));
            THROW_EXCEPTION(AppendStoreCompressionException, "decompression codec error");
        }
    }
    catch (ExceptionBase& e)
    {
        LOG4CXX_ERROR(logger_, "Error " << e.ToString());
        throw;
    }

    return true;
}

OffsetType Chunk::AppendRaw(const IndexType& index, const uint32_t numentry, const std::string& data)
{
	OffsetType result = -1;
    /*
      try
      {
      Timer t;
      t.start();
      OffsetType fos = 0;            
      // std::cout << "\nactual data " << data.size(); // << ", data wrote : " << ssref.size();          
      fos = mDataOutputFH->Write((char*)&data[0], data.size()); 
      //LOG4CXX_DEBUG(logger_, "flush -- data wrote ------- " << ssref);
      //LOG4CXX_DEBUG(logger_, "flush -- data size wrote -- " << ssref.size());
      LOG4CXX_DEBUG(logger_, "flush return value is ----- " << fos);
      cout << endl << "Write done " << t.stop() << " ms";
      result = fos;
      }
      //catch(StreamCorruptedException& e)
      catch(ExceptionBase& e)
      {
      LOG4CXX_ERROR(logger_, "Exception while writing");
      }
      //cout << "Time Chunk::AppendRaw : " << t.stop << " ms";
      return result;
      }

      // compress data (data consists of multiple records)
      //Timer t;
      OffsetType result = -1;
      //t.start();
      //
      */
    CompressionCodecPtr sharedptr = mChunkCodec.lock();
    if (sharedptr == NULL) 
    {
        LOG4CXX_ERROR(logger_, ("Error : the compression codec has been destructed."));
        THROW_EXCEPTION(AppendStoreCompressionException, "compression error inside AppendRaw()");
    }

    uint32_t bufsize = sharedptr->getBufferSize(data.size());
    std::string sbuf;
    sbuf.resize(bufsize);
    uint32_t compressedSize;
    int retc = sharedptr->compress((char*)&data[0], data.size(), &sbuf[0], compressedSize);
    if (retc < 0) 
    {
        LOG4CXX_ERROR(logger_, ("Error : error when compressing data"));
        THROW_EXCEPTION(AppendStoreCompressionException, "compression error inside AppendRaw()");
    }

    CompressedDataRecord crd(index, numentry, data.size(), compressedSize, sbuf);
    std::stringstream ssbuf;
    crd.Serialize(ssbuf);
    const std::string& ssref = ssbuf.str();

    // may retry once
    int32_t retryCount = 0;
    do
    {
        try
        {
            // CHKIT
            OffsetType fos = 0;            
            // std::cout << "\nactual data " << data.size() << ", data wrote : " << ssref.size();	
            fos = mDataOutputFH->Write((char*)&ssref[0], ssref.size());	
            //LOG4CXX_DEBUG(logger_, "flush -- data wrote ------- " << ssref);
            LOG4CXX_DEBUG(logger_, "flush -- data size wrote -- " << ssref.size());
            LOG4CXX_DEBUG(logger_, "flush return value is ----- " << fos);
            LOG4CXX_WARN(logger_, "Data Flushed : " << ssref.size());
            // cout << endl << "Compression and Flush took " << t.stop() << " ms";
            result = fos;
            break;
        }
        //catch(StreamCorruptedException& e)
        catch(ExceptionBase& e)
        {
               
            LOG4CXX_ERROR(logger_, "DataOutputStream FlushLog fail : " << e.ToString());
            try
            {
                mDataOutputFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG4CXX_ERROR(logger_, "Failed close file after write fail : " << e.ToString());
            }
            // mDataOutputFH->Seek(0); // .reset();

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }

            mDataOutputFH = mFileSystemHelper->CreateFileHelper(mDataFileName, O_APPEND); //WRONLY
            mDataOutputFH->Open();
            
            if (retryCount > 1)
            {
                LOG4CXX_ERROR(logger_, "DataOutputStream FlushLog fail after retry : " << e.ToString());
                throw;
            }
        }
    } while (retryCount <= 1);

    //cout << endl << "Time Chunk::AppendRaw() Compressed : " << t.stop() << " ms";
    return result;
}

