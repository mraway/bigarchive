#include "append_store_chunk.h"
#include "exception.h"

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
    // CHKIT : get the host and port from config file
    mFileSystemHelper = new QFSHelper();    
    mFileSystemHelper->Connect("localhost", 30000);
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
	// CHKIT : get the host and port from config file
	mFileSystemHelper = new QFSHelper;
	mFileSystemHelper->Connect("hostname", 30000);
	LoadDeleteLog();
}

void Chunk::LoadDeleteLog()
{
	// CHKIT
	mDeleteLogFH = new QFSFileHelper((QFSHelper*) mFileSystemHelper, mLogFileName, O_WRONLY);
	if(mFileSystemHelper->IsFileExists(mLogFileName) == false) {
		mDeleteLogFH->Create();
	}
	mDeleteLogFH->Open();
}

void Chunk::CheckIfNew()
{
    bool dexist;
    bool iexist;
    try
    {
        dexist = mFileSystemHelper->IsFileExists(mDataFileName); // PanguHelper::IsFileExist(mDataFileName);
        iexist = mFileSystemHelper->IsFileExists(mIndexFileName); // PanguHelper::IsFileExist(mIndexFileName);
    }
    catch(ExceptionBase& e)
    {
        LOG4CXX_ERROR(logger, "Error in CheckIfNew: " << e.ToString());
        throw;
    }
    if (! ((dexist && iexist) || (!dexist && !iexist)) )
    {
        LOG4CXX_ERROR(logger, "mDataFileName and mIndexFileName not co-exist");
        THROW_EXCEPTION(ExceptionBase, "mDataFileName and mIndexFileName not co-exist");
    }

    if (!dexist && !iexist)
    {
	QFSFileHelper *t;
       	t = new QFSFileHelper((QFSHelper*)mFileSystemHelper, mDataFileName, O_WRONLY); // WRITE
	t->Create();
       	t = new QFSFileHelper((QFSHelper*)mFileSystemHelper, mIndexFileName, O_WRONLY); // WRITE
        t->Create();
    }
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
        ss<< "Chunk is full but still used somehow ";
        THROW_EXCEPTION(AppendStoreWriteException, ss.str());
    }
    if (mBlockStream.str().size()+data.size() >= DF_MAX_BLOCK_SZ)
    {
        AppendIndex();
    }
    IndexType new_index = GenerateIndex();

    DataRecord r(data, new_index);
    r.Serialize(mBlockStream);
    mFlushCount++;
    if (mFlushCount >= mBlockIndexInterval)
    {
        AppendIndex();
    }
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

    std::stringstream streamBuf;
    r.Serialize(streamBuf);


    int32_t retryCount = 0;
    do
    {
        try
        {
            mIndexOutputFH->Flush((char*)&streamBuf.str()[0], streamBuf.str().size());
            break;
        }
        catch(StreamCorruptedException& e)
        {
            LOG4CXX_ERROR(logger, "IndexOutputStream corrupt << e.ToString()");
            try
            {
                mIndexOutputFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG4CXX_ERROR(logger, "Failed close file after write fail " << e.ToString());
            }
            
            mIndexOutputFH->Seek(0);

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }

            // CHKIT -> from WRITE mode to APPEND mode
            mIndexOutputFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, mIndexFileName, O_APPEND); // O_WRONLY); // WRITE
            mIndexOutputFH->Open();

            if (retryCount > 1)
            {
                LOG4CXX_ERROR(logger, "DataOutputStream FlushLog fail after retry " << e.ToString());
                throw;
            }
        }
        catch (ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger, "IndexOutputStream FlushLog fail" << e.ToString());
            throw;
        }
    } while (retryCount <= 1);
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
    OffsetType startOffset = it->mOffset ;
    std::string buf;
    bool ret = ReadRaw(startOffset, buf);
    ret = ret && ExtractDataFromBlock(buf, index, data);
    return ret;
}

bool Chunk::ExtractDataFromBlock(const std::string& buf, IndexType index, std::string* data)
{
    CachePtr cachesharedptr = mCachePtr.lock();
    if (cachesharedptr == NULL)
    {
	// CHKIT
        LOG4CXX_ERROR(logger, "the cache has been destructed.");
        THROW_EXCEPTION(AppendStoreReadException, "Failed to get cachePtr");
    }

    bool ret = false;
    cachesharedptr->Clear();
    std::stringstream streamBuf(buf);
    do
    {
	// CHKIT
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
            data->clear();   // only the last block with same index will be returned
            data->append(r.mVal);
            ret = true;
        }
        cachesharedptr->Insert(Handle(mChunkId, r.mIndex), r.mVal);
    } while(true);

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
        catch(StreamCorruptedException& e)
        {
            LOG4CXX_ERROR(logger, "DeleteLogStream corrupt " << e.ToString());
            try
            {
            	mDeleteLogFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG4CXX_ERROR(logger, "Failed close file after write fail " << e.ToString());
            }
            mDeleteLogFH->Seek(0);

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }
        
            mDeleteLogFH = new QFSFileHelper((QFSHelper*)mFileSystemHelper, mLogFileName, O_APPEND); // WRITE);

            if (retryCount > 1)
            {
                LOG4CXX_ERROR(logger, "DataOutputStream FlushLog fail after retry " << e.ToString());
                throw;
            }
        }
        catch (ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger, "DeleteLogStream FlushLog fail " << e.ToString());
            throw;
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
            mLastData = mFileSystemHelper->getSize(mDataFileName);
        }
        catch(ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger, "error on get data file size" << e.ToString());
            throw;
        }

        mDataOutputFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, mDataFileName, O_WRONLY);// WRITE);
        mDataOutputFH->Open();
        mIndexOutputFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, mIndexFileName, O_WRONLY); //WRITE);
        mIndexOutputFH->Open();
    }
    else 
    {
    	mDataInputFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, mDataFileName, O_RDONLY); // READ);
    	mDataInputFH->Open();
    }
    return true;
}

uint32_t Chunk::GetChunkSize(const std::string& root, ChunkIDType chunk_id)
{
    std::string dat_file = GetDatFname(root, chunk_id);
    FileSystemHelper *fsh = new QFSHelper();
    fsh->Connect("host", 30000);
    return fsh->getSize(dat_file);
}

uint16_t Chunk::GetMaxChunkID(const std::string& root)
{
    std::string data_root = root + Defaults::DAT_DIR;
    std::vector<std::string> data_files; 
    try
    {
    	// CHECK IT
	FileSystemHelper *fsh;
	fsh = new QFSHelper();
	fsh->Connect("host", 30000);
        fsh->ListDir(data_root, data_files);
    }
    catch(ExceptionBase& e)
    {
        LOG4CXX_ERROR(logger, "Error " << e.ToString());
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
    if (mDataInputFH) {
        try
        {
            mDataInputFH->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger, "Failed to close input data file" << ex.ToString());
        }
        mDataInputFH->Seek(0); // .reset();
    }
    if (mDataOutputFH)
    {
        try
        {
            mDataOutputFH->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger, "Failed to close output data file " << ex.ToString());
        }
        mDataOutputFH->Seek(0); // .reset();
    }
    if (mIndexOutputFH)
    {
        try
        {
            mIndexOutputFH->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger, "Failed to close output index file " << ex.ToString());
        }
        mIndexOutputFH->Seek(0); //reset();
    }
    if (mDeleteLogFH)
    {
        try
        {
            mDeleteLogFH->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger, "Failed to close deletelog file" <<  ex.ToString());
        }
        mDeleteLogFH->Seek(0); // .reset();
    }

    return true;
}


bool Chunk::ReadRaw(const OffsetType& offset, std::string& data) 
{
    try
    {
        mDataInputFH->Seek(offset);
        // what is next log size ???
        // CHECK WITH WEI : CHKIT
        uint32_t read_len = mDataInputFH->GetNextLogSize();
        std::string blkdata;
        blkdata.resize(read_len, 0);
        uint32_t size = 0;
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
            LOG4CXX_ERROR(logger, "Error : the compression codec has been destructed.");
            THROW_EXCEPTION(AppendStoreCompressionException, "decompression error inside ReadRaw()");
        }

        uint32_t uncompressedSize;
        data.resize(crd.mOrigLength);
        int retc = sharedptr->decompress(&(crd.mData[0]), crd.mCompressLength, &data[0], uncompressedSize);
        if (uncompressedSize != crd.mOrigLength)
        {
            LOG4CXX_ERROR(logger, ("Error : error when decompressing due to invalid length"));
            THROW_EXCEPTION(AppendStoreCompressionException, "decompression invalid length");
        }
        if (retc < 0)
        {
            LOG4CXX_ERROR(logger, ("Error : decompression codec error when decompressing inside ReadRaw()"));
            THROW_EXCEPTION(AppendStoreCompressionException, "decompression codec error");
        }
    }
    catch (ExceptionBase& e)
    {
        LOG4CXX_ERROR(logger, "Error " << e.ToString());
        throw;
    }

    return true;
}

OffsetType Chunk::AppendRaw(const IndexType& index, const uint32_t numentry, const std::string& data)
{
    // compress data (data consists of multiple records)
    CompressionCodecPtr sharedptr = mChunkCodec.lock();
    if (sharedptr == NULL) 
    {
        LOG4CXX_ERROR(logger, ("Error : the compression codec has been destructed."));
        THROW_EXCEPTION(AppendStoreCompressionException, "compression error inside AppendRaw()");
    }

    uint32_t bufsize = sharedptr->getBufferSize(data.size());
    std::string sbuf;
    sbuf.resize(bufsize);
    uint32_t compressedSize;
    int retc = sharedptr->compress((char*)&data[0], data.size(), &sbuf[0], compressedSize);
    if (retc < 0) 
    {
        LOG4CXX_ERROR(logger, ("Error : error when compressing data"));
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
	    fos = mDataOutputFH->Flush((char*)&ssref[0], ssref.size());
            return fos;
        }
        catch(StreamCorruptedException& e)
        {
            LOG4CXX_ERROR(logger, "DataOutputStream corrupt : " << e.ToString());
            try
            {
                mDataOutputFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG4CXX_ERROR(logger, "Failed close file after write fail : " << e.ToString());
            }
            mDataOutputFH->Seek(0); // .reset();

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }

            mDataOutputFH = new QFSFileHelper((QFSHelper*)mFileSystemHelper, mDataFileName, O_APPEND); //APPEND
            mDataOutputFH->Open();
            
            if (retryCount > 1)
            {
                LOG4CXX_ERROR(logger, "DataOutputStream FlushLog fail after retry : " << e.ToString());
                throw;
            }
        }
        catch(ExceptionBase& e)
        {
            LOG4CXX_ERROR(logger, "DataOutputStream FlushLog fail : " << e.ToString());
            throw;
        }
    } while (retryCount <= 1);

    return 0;
}

