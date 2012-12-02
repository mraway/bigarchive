#include "append_store_chunk.h"
#include "append_store_exception.h"

using namespace apsara;
using namespace apsara::pangu;
using namespace apsara::AppendStore;


const char* AppendStore::Defaults::IDX_DIR = "index/";
const char* AppendStore::Defaults::DAT_DIR = "data/";
const char* AppendStore::Defaults::LOG_DIR = "log/";

// apsara::logging::Logger* AppendStore::Chunk::sLogger = apsara::logging::GetLogger("/apsara/append_store");

AppendStore::Chunk::Chunk(const std::string& root, ChunkIDType chunk_id, 
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
	// change required
    mFileSystemHelper = FileSystemHelper::GetInstance();
    CheckIfNew();
    LoadIndex();
    LoadData(append_flag);
}

AppendStore::Chunk::Chunk(const std::string& root, ChunkIDType chunk_id)
    : mRoot(root), 
    mChunkId(chunk_id), 
    mLogFileName(GetIdxLogFname(root, chunk_id)), 
    mDirty(false)
{
	// change required
    mFileSystemHelper = FileSystemHelper::GetInstance();
    LoadDeleteLog();
}

void AppendStore::Chunk::LoadDeleteLog()
{
	// QFSHelper qfsHelper = new QFSHelper();
	// qfsHelper.connect();
	mDeleteLogFH = new QFSFileHelper(qfsHelper, mLogFileName, WRITE);
	if(QFSFileHelper::IsFileExist(mLogFileName) == false) {
		mDeleteLogFH.Create();
	}
	mDeleteLogFH.Open();
}

void AppendStore::Chunk::CheckIfNew()
{
    bool dexist;
    bool iexist;
    try
    {
        dexist = QFSFileHelper::IsFileExist(mDataFileName); // PanguHelper::IsFileExist(mDataFileName);
        iexist = QFSFileHelper::IsFileExist(mIndexFileName); // PanguHelper::IsFileExist(mIndexFileName);
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in CheckIfNew: ", e.ToString()));
        throw;
    }
    if (! ((dexist && iexist) || (!dexist && !iexist)) )
    {
        LOG_ERROR(sLogger, ("Error: ", "mDataFileName and mIndexFileName not co-exist"));
        APSARA_THROW(ExceptionBase, "mDataFileName and mIndexFileName not co-exist");
    }

    if (!dexist && !iexist)
    {
        try
        {
        	mDataLogFH = new QFSFileHelper(qfsHelper, mDataFileName, WRITE);
        	mIndexLogFH = new QFSFileHelper(qfsHelper, mIndexFileName, WRITE);
        	mDataLogFH->Create();
        	mIndexLogFH->Create();
            // PanguHelper::CreateLogFile(mDataFileName, DF_MINCOPY, DF_MAXCOPY, AppName, PartName);
            // PanguHelper::CreateLogFile(mIndexFileName, DF_MINCOPY, DF_MAXCOPY, AppName, PartName);
        }
        catch(ExceptionBase& e)
        {
            LOG_ERROR(sLogger, ("Error in CheckIfNew: ", e.ToString()));
            throw;
        }
    }
}
   
AppendStore::Chunk::~Chunk()
{
    Flush();
    Close();
}

void AppendStore::Chunk::Flush()
{
    if (mDirty)
    {
        if (mFlushCount > 0)
        {
            AppendIndex();
        }
    }
}

AppendStore::IndexType AppendStore::Chunk::Append(const std::string& data)
{
    if (IsChunkFull() == true)
    {
        std::stringstream ss;
        ss<<"Chunk is full but still used somehow ";
        APSARA_THROW(AppendStoreWriteException, ss.str());
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

void AppendStore::Chunk::AppendIndex()
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

    // may retry once 
    int32_t retryCount = 0;
    do
    {
        try
        {
        	mIndexOutputFH->Write((char*)&streamBuf.str()[0], streamBuf.str().size());
        	// mIndexOutputStream->FlushLog((char*)&streamBuf.str()[0], streamBuf.str().size());
            break;
        }
        catch(StreamCorruptedException& e)
        {
            LOG_ERROR(sLogger, ("IndexOutputStream corrupt", e.ToString()));
            try
            {
                mIndexOutputFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG_ERROR(sLogger, ("Failed close file after write fail", e.ToString()));
            }
            // mIndexOutputStream.reset();
            mIndexOutputFH->Seek(0);

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }

            // mIndexOutputStream = PanguHelper::OpenLog4Append(mIndexFileName);
            mIndexOutputFH = new QFSFileHelper(qfsHelper, mIndexFileName, WRITE);
            mIndexOutputFH->Open();

            if (retryCount > 1)
            {
                LOG_ERROR(sLogger, ("DataOutputStream FlushLog fail after retry", e.ToString()));
                throw;
            }
        }
        catch (ExceptionBase& e)
        {
            LOG_ERROR(sLogger, ("IndexOutputStream FlushLog fail", e.ToString()));
            throw;
        }
    } while (retryCount <= 1);
};
       

bool AppendStore::Chunk::Read(AppendStore::IndexType index, std::string* data) 
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

bool AppendStore::Chunk::ExtractDataFromBlock(const std::string& buf, AppendStore::IndexType index, std::string* data)
{
    CachePtr cachesharedptr = mCachePtr.lock();
    if (cachesharedptr == NULL)
    {
        LOG_ERROR(sLogger, ("Error", "the cache has been destructed."));
        APSARA_THROW(AppendStoreReadException, "Failed to get cachePtr");
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
            APSARA_THROW(AppendStoreReadException, "Failed extracting data from block");
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

bool AppendStore::Chunk::Remove(const AppendStore::IndexType& index)
{
    DeleteRecord d(index);
    std::string tmp = d.ToString(); 

    // may retry once 
    int32_t retryCount = 0;
    do
    {
        try
        {
        	mDeleteLogFH->Write(&tmp[0], tmp.size());
            // mDeleteLogStream->FlushLog(&tmp[0], tmp.size());
            break;
        }
        catch(StreamCorruptedException& e)
        {
            LOG_ERROR(sLogger, ("DeleteLogStream corrupt", e.ToString()));
            try
            {
                // mDeleteLogStream->Close();
            	mDeleteLogFH->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG_ERROR(sLogger, ("Failed close file after write fail", e.ToString()));
            }
            // mDeleteLogStream.reset(); CHECK
            mDeleteLogFH->Seek(0);

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }
            // mDeleteLogStream = PanguHelper::OpenLog4Append(mLogFileName);
            mDeleteLogFH = new QFSFileHelper(qfsHelper, mLogFileName, WRITE);

            if (retryCount > 1)
            {
                LOG_ERROR(sLogger, ("DataOutputStream FlushLog fail after retry", e.ToString()));
                throw;
            }
        }
        catch (ExceptionBase& e)
        {
            LOG_ERROR(sLogger, ("DeleteLogStream FlushLog fail", e.ToString()));
            throw;
        }
    } while (retryCount <= 1);

    return true;
}

bool AppendStore::Chunk::LoadIndex()
{
    mIndexMap.reset(new IndexVector(mIndexFileName));
    uint32_t size = mIndexMap->size();
    if (size > 0)
    {
        mMaxIndex = mIndexMap->at(size-1).mIndex;
    }
    return true;
}

bool AppendStore::Chunk::LoadData(bool flag)
{
    if (flag)
    {
        try
        {
            mLastData = QFSFileHelper::GetFileSize(mDataFileName);
        }
        catch(ExceptionBase& e)
        {
            LOG_ERROR(sLogger, ("error on get data file size", e.ToString()));
            throw;
        }
        // mDataOutputStream  = PanguHelper::OpenLog4Append(mDataFileName);
        // mIndexOutputStream = PanguHelper::OpenLog4Append(mIndexFileName);
        mDataOutputFH = new QFSFileHelper(qfsHelper, mDataFileName, WRITE);
        mDataOutputFH->Open();
        mIndexOutputFH = new QFSFileHelper(qfsHelper, mIndexFileName, WRITE);
        mIndexOutputFH->Open();
    }
    else 
    {
    	mDataInputFH = new QFSFileHelper(qfsHelper, mDataFileName, READ);
    	mDataInputFH->Open();
        // mDataInputStream = PanguHelper::OpenLog4Read(mDataFileName);
    }
    return true;
}

uint32_t AppendStore::Chunk::GetChunkSize(const std::string& root, ChunkIDType chunk_id)
{
    std::string dat_file = GetDatFname(root, chunk_id);
    return QFSFileHelper::GetFileSize(dat_file);
}

uint16_t AppendStore::Chunk::GetMaxChunkID(const std::string& root)
{
    std::string data_root = root + Defaults::DAT_DIR;

    // scan all files from the AppendStore data directory.
    std::vector<std::string> data_files; 
    try
    {

        apsara::pangu::FileSystem::GetInstance()->ListDirectory(data_root, "", DF_MAXFILENO, data_files);
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error", e.ToString()));
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

bool AppendStore::Chunk::IsChunkFull() const
{
    uint64_t size = (mLastData& 0xFFFFFFFF)+((mLastData >> 32) & 0xFFFFFFFF)*64*1024*1024;
    return ((size >= mMaxChunkSize)||(mMaxIndex==(IndexType)-1));
}

inline AppendStore::IndexType AppendStore::Chunk::GenerateIndex()
{
    if (mMaxIndex != IndexType(-1)) 
    {
        ++mMaxIndex;
        return mMaxIndex;
    }
    APSARA_THROW(AppendStoreInvalidIndexException, "index overflow during GenerateIndex()");

}

bool AppendStore::Chunk::IsValid(const IndexType& value)
{
    return ((value&0xc000000000000000llu)==0);
}

std::string AppendStore::Chunk::GetIdxFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%sidx.%x",root.c_str(), Defaults::IDX_DIR, chunk_id);
    return buf;
}

std::string AppendStore::Chunk::GetIdxLogFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%slog.%x",root.c_str(), Defaults::LOG_DIR, chunk_id);
    return buf;
}

std::string AppendStore::Chunk::GetDatFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%sdat.%x",root.c_str(), Defaults::DAT_DIR, chunk_id);
    return buf;
}

std::string AppendStore::Chunk::GetLogFname(const std::string& root, uint32_t chunk_id)
{
    char buf[1024];
    sprintf(buf, "%s%slog.%x",root.c_str(), Defaults::LOG_DIR, chunk_id);
    return buf;
}


bool AppendStore::Chunk::Close()
{
    if (mDataInputStream) {
        try
        {
            mDataInputStream->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG_ERROR(sLogger, ("Failed to close input data file", ex.ToString()));
        }
        mDataInputStream.reset();
    }
    if (mDataOutputStream)
    {
        try
        {
            mDataOutputStream->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG_ERROR(sLogger, ("Failed to close output data file", ex.ToString()));
        }
        mDataOutputStream.reset();
    }
    if (mIndexOutputStream)
    {
        try
        {
            mIndexOutputStream->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG_ERROR(sLogger, ("Failed to close output index file", ex.ToString()));
        }
        mIndexOutputStream.reset();
    }
    if (mDeleteLogStream)
    {
        try
        {
            mDeleteLogStream->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG_ERROR(sLogger, ("Failed to close deletelog file", ex.ToString()));
        }
        mDeleteLogStream.reset();
    }

    return true;
}


bool AppendStore::Chunk::ReadRaw(const OffsetType& offset, std::string& data) 
{
    try
    {
        mDataInputStream->Seek(offset); 
        uint32_t read_len = mDataInputStream->GetNextLogSize();
        std::string blkdata;
        blkdata.resize(read_len, 0);
        uint32_t size = mDataInputStream->ReadLog(&blkdata[0], read_len);

        if (read_len != size)
        {
            std::stringstream ss;
            ss<<"DataInputStream file read error, need size: " << size <<" actual size: "<<read_len;
            APSARA_THROW(AppendStoreReadException, ss.str());
        }

        CompressedDataRecord crd;
        std::stringstream    sstream(blkdata);
        crd.Deserialize(sstream);

        // decompress 
        CompressionCodecPtr sharedptr = mChunkCodec.lock();
        if (sharedptr == NULL)
        {
            LOG_ERROR(sLogger, ("Error", "the compression codec has been destructed."));
            APSARA_THROW(AppendStoreCompressionException, "decompression error inside ReadRaw()");
        }

        uint32_t uncompressedSize;
        data.resize(crd.mOrigLength);
        int retc = sharedptr->decompress(&(crd.mData[0]), crd.mCompressLength, &data[0], uncompressedSize);
        if (uncompressedSize != crd.mOrigLength)
        {
            LOG_ERROR(sLogger, ("Error", "error when decompressing due to invalid length"));
            APSARA_THROW(AppendStoreCompressionException, "decompression invalid length");
        }
        if (retc < 0)
        {
            LOG_ERROR(sLogger, ("Error", "decompression codec error when decompressing inside ReadRaw()"));
            APSARA_THROW(AppendStoreCompressionException, "decompression codec error");
        }
    }
    catch (ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error", e.ToString()));
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
        LOG_ERROR(sLogger, ("Error", "the compression codec has been destructed."));
        APSARA_THROW(AppendStoreCompressionException, "compression error inside AppendRaw()");
    }

    uint32_t bufsize = sharedptr->getBufferSize(data.size());
    std::string sbuf;
    sbuf.resize(bufsize);
    uint32_t compressedSize;
    int retc = sharedptr->compress((char*)&data[0], data.size(), &sbuf[0], compressedSize);
    if (retc < 0) 
    {
        LOG_ERROR(sLogger, ("Error", "error when compressing data"));
        APSARA_THROW(AppendStoreCompressionException, "compression error inside AppendRaw()");
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
            OffsetType fos = mDataOutputStream->FlushLog((char*)&ssref[0], ssref.size());
            return fos;
        }
        catch(StreamCorruptedException& e)
        {
            LOG_ERROR(sLogger, ("DataOutputStream corrupt", e.ToString()));
            try
            {
                mDataOutputStream->Close();
            }
            catch(ExceptionBase& e)
            {
                LOG_ERROR(sLogger, ("Failed close file after write fail", e.ToString()));
            }
            mDataOutputStream.reset();

            if (++retryCount <= 1)
            {
                usleep(3000000);
            }
            mDataOutputStream = PanguHelper::OpenLog4Append(mDataFileName);
            
            if (retryCount > 1)
            {
                LOG_ERROR(sLogger, ("DataOutputStream FlushLog fail after retry", e.ToString()));
                throw;
            }
        }
        catch(ExceptionBase& e)
        {
            LOG_ERROR(sLogger, ("DataOutputStream FlushLog fail", e.ToString()));
            throw;
        }
    } while (retryCount <= 1);

    return 0;
}

