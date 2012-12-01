#include <algorithm>
#include <set>
#include "apsara/pgfstream.h"
#include "pangu_helper.h"
#include "append_store.h"
#include "append_store_cj.h"
#include "append_store_exception.h"


using namespace apsara;
using namespace apsara::pangu;
using namespace apsara::security;
using namespace apsara::AppendStore;


apsara::logging::Logger* PanguAppendStore::sLogger = apsara::logging::GetLogger("/apsara/append_store");


PanguAppendStore::PanguAppendStore(const StoreParameter& para, bool iscreate)
    : mRoot(para.mPath), 
     mAppend(para.mAppend), 
     mMaxChunkId(0), 
     mAppendChunkId(0),
     mCompressionType(para.mCompressionFlag)
{
    if (mRoot.compare(mRoot.size()-1, 1, "/"))
    {
        mRoot.append("/");
    }

    mMeta = StoreMetaData(MAJOR_VER, MINOR_VER, para.mMaxChunkSize, para.mBlockIndexInterval, para.mCompressionFlag);
    if (para.mMaxChunkSize == 0)
    {
        mMeta.maxChunkSize = AppendStore::DF_CHUNK_SZ;
    }
    if (para.mBlockIndexInterval == 0)
    {
        mMeta.blockIndexInterval = AppendStore::DF_MAX_PENDING;
    }
    Init(iscreate);
}

PanguAppendStore::~PanguAppendStore()
{
    //UninitPangu();
}

Scanner* PanguAppendStore::GetScanner()
{
    return new PanguScanner(mRoot, mMeta.compressionFlag);
}

std::string PanguAppendStore::Append(const std::string& data)
{
    if (!mAppend)
    {
        APSARA_THROW(AppendStoreWriteException, "Cannot append for read-only store");
    }

    Chunk* p_chunk = LoadAppendChunk();
    Handle h;
    h.mIndex = p_chunk->Append(data);
    LOG_DEBUG(sLogger, ("Write", mRoot) ("mChunkId", p_chunk->GetID()) ("mIndex", h.mIndex) ("size", data.size()));

    if (h.mIndex==0)
    {
        APSARA_THROW(AppendStoreWriteException, "Wrong handle index ==0");
    }
    h.mChunkId = p_chunk->GetID();
    return h.ToString();
}

// TODO: improve implementation instead of simple loop
//
void PanguAppendStore::BatchAppend(const std::vector<std::string>& datavec, std::vector<std::string>& handlevec) 
{
    handlevec.clear();

    uint32_t vsize = datavec.size();
    if (vsize > 0) 
    {
        for (uint32_t i=0; i<vsize; ++i) 
        {
            std::string hd = Append(datavec[i]);
            handlevec.push_back(hd);
        }
        Flush();
    }
}

bool PanguAppendStore::ValidChunkID(ChunkIDType id) const
{
    return id <= mMaxChunkId;
}

// not implemented yet
void PanguAppendStore::GarbageCollection(bool force)
{
    mChunkMap.clear();
}

void PanguAppendStore::Flush()
{
    if (mCurrentAppendChunk.get() != 0)
    {
        mCurrentAppendChunk->Flush();
    }
}

bool PanguAppendStore::Read(const std::string& h, std::string* data) 
{
    bool bOK = false;
    Handle handle(h);
    if (handle.isValid() == false)
    {
        return bOK;
    }

    if (mCache->Find(handle, data))
    {
        return true;
    }

    LOG_DEBUG(sLogger, ("READ -", mRoot) ("mChunkId", handle.mChunkId) ("mIndex", handle.mIndex));
    Chunk* p_chunk = LoadRandomChunk(handle.mChunkId);
    if (p_chunk == 0)
    {
        return false;
    }

    bOK = p_chunk->Read(handle.mIndex, data);
    LOG_DEBUG(sLogger, ("READ", mRoot) ("mChunkId", handle.mChunkId) ("mIndex", handle.mIndex));

    return bOK;
}

void PanguAppendStore::Remove(const std::string& h)
{
    Handle handle(h);
    if (handle.isValid() == false)
    {
        return;
    }
    Chunk* p_chunk = LoadDeleteChunk(handle.mChunkId);
    if (p_chunk == 0)
    {
        return ;
    }
    p_chunk->Remove(handle.mIndex);
}

void PanguAppendStore::Reload()
{
    mChunkMap.clear();
    mCache->Clear();
    mMaxChunkId = Chunk::GetMaxChunkID(mRoot);

    mCurrentRandomChunk.reset();
}

void PanguAppendStore::Init(bool iscreate)
{
    InitPangu();
    mFileSystemPtr = FileSystem::GetInstance();

    // create the outer directory and index, data and log file directories.
    if (iscreate) 
    {
        CreateDirs(mRoot);
    }

    uint32_t binmajor = mMeta.storemajor; 
    uint32_t binminor = mMeta.storeminor;
    if (ReadMetaInfo())
    {
        if (!mMeta.check(binmajor, binminor))
        {
            APSARA_THROW(AppendStoreMisMatchedVerException, "meta version is not matched");
        }
        mCompressionType = mMeta.compressionFlag;
    }
    else
    {
        if (iscreate)
        { 
            WriteMetaInfo(mRoot, mMeta);
        } 
        else {
            APSARA_THROW(AppendStoreNotExistException, "store not exist (.meta_)" + mRoot);
        }
    }
    std::string compressAlgo(LzoCodec::mName);
    if (mCompressionType == (uint32_t)NO_COMPRESSION) 
    {
        compressAlgo = NoneCodec::mName;
    }
    else if (mCompressionType != (uint32_t)COMPRESSOR_LZO) 
    {
        APSARA_THROW(AppendStoreCodecException, "unknown compression type");
    }

    // directory exist 
    if (!CheckDirs(mRoot))
    {
        APSARA_THROW(AppendStoreNotExistException, "store not exist (3 dirs)" + mRoot);
    }
    mMaxChunkId = Chunk::GetMaxChunkID(mRoot);

    if (mAppend)
    {
        LOG_INFO(sLogger, ("mMaxChunkSize", mMeta.maxChunkSize)
                          ("mBlockIndexInterval", mMeta.blockIndexInterval));

        // Currently, append at the last chunk
        //set mMaxChunkId: chunks are in [0, mMaxChunkId] inclusive
        mAppendChunkId = mMaxChunkId;
        mCodec.reset(CompressionCodec::getCodec(compressAlgo.c_str(), 1024, true));
    }
    else 
    {
        mCodec.reset(CompressionCodec::getCodec(compressAlgo.c_str(), 1024, false));
    }

    mCache.reset(new Cache());
}


bool PanguAppendStore::ReadMetaInfo()
{
    std::string metaFileName = mRoot + AppendStore::MetaFileName;

    bool fexist = false; 
    try  
    {
        fexist = PanguHelper::IsFileExist(metaFileName);
    }
    catch (ExceptionBase & e)
    {
        LOG_ERROR(PanguAppendStore::sLogger, ("IsFileExist", metaFileName));
        throw;
    }

    if (fexist)
    {
        try
        {
            pgifstream iff;
            iff.open(metaFileName);
            AppendStoreMeta meta;
            meta.FromStream(iff);
            iff.close();

            mMeta.storemajor   = meta.GetMajor();
            mMeta.storeminor   = meta.GetMinor();
            mMeta.maxChunkSize = meta.GetMaxChunkSize();
            mMeta.blockIndexInterval = meta.GetBlockIndexInterval();
            mMeta.compressionFlag    = (DataFileCompressionFlag)meta.GetCompressType();

            return true;
        }
        catch (apsara::ExceptionBase& e)
        {
            APSARA_THROW(AppendStoreWriteException, "Cannot open meta file for append " + e.ToString());
        }
    }
    return false;
}

void PanguAppendStore::WriteMetaInfo(const std::string& root, const StoreMetaData& meta)
{
    std::string metaFileName = root + AppendStore::MetaFileName;
    try
    {
        pgofstream off;
        off.setOption(DF_MINCOPY, DF_MAXCOPY, "", "");

        off.open(metaFileName, true);
        AppendStoreMeta savemeta;
        savemeta.SetMajor(meta.storemajor);
        savemeta.SetMinor(meta.storeminor);
        savemeta.SetMaxChunkSize(meta.maxChunkSize);
        savemeta.SetBlockIndexInterval(meta.blockIndexInterval);
        savemeta.SetCompressType(meta.compressionFlag);

        savemeta.ToStream(off);
        off.flush();
        off.close();
    }
    catch (ExceptionBase& e) 
    {
        APSARA_THROW(AppendStoreWriteException, e.ToString()+" Cannot generate .meta_ file");
    }
}

void PanguAppendStore::AllocNextChunk()
{
    ++mMaxChunkId;
    mAppendChunkId = mMaxChunkId;
}

Chunk* PanguAppendStore::LoadAppendChunk()
{
    if (mCurrentAppendChunk.get() != 0)
    {
        if (mCurrentAppendChunk->IsChunkFull() == false)
        {
            return mCurrentAppendChunk.get();
        }
        else
        {
            AllocNextChunk();

        }
    }
    try
    {
        // Create an appendchunk.
        mCurrentAppendChunk.reset(new Chunk(mRoot, mAppendChunkId, mMeta.maxChunkSize, true, mCodec, mCache, mMeta.blockIndexInterval));
    }
    catch (...)
    {
        LOG_ERROR(sLogger, ("[AppendStore] %d chunk is disabled for append", mAppendChunkId));
        throw;
    }

    if (0 == mCurrentAppendChunk.get())
    {
        APSARA_THROW(AppendStoreWriteException, "Cannot get valid chunk for append");
    }
    return mCurrentAppendChunk.get();
}

Chunk* PanguAppendStore::LoadRandomChunk(ChunkIDType id) 
{
    if (ValidChunkID(id) == false)
    {
        return 0;
    }
/*
    if(mCurrentAppendChunk.get() != 0 && mCurrentAppendChunk->GetID() == id)
    {
        APSARA_THROW(AppendStoreReadException, "Trying to read a chunk that is being modified, abort...");
    }
*/
    ChunkMapType::const_iterator it = mChunkMap.find(id);
    if (it != mChunkMap.end())
    {
        return it->second.get();
    }

/*
    if(Chunk::GetChunkSize(mRoot, id) == (uint32_t)-1)
    {
        return 0;
    }
*/

    // create a non-append chunk.
    mCurrentRandomChunk.reset(new Chunk(mRoot, id, mMeta.maxChunkSize, false, mCodec, mCache));
    assert(mCurrentRandomChunk.get());
    mChunkMap.insert(std::make_pair(id, mCurrentRandomChunk));
    return mCurrentRandomChunk.get();
}

Chunk* PanguAppendStore::LoadDeleteChunk(ChunkIDType id) 
{
    if (ValidChunkID(id) == false)
    {
        return 0;
    }
    ChunkMapType::const_iterator it = mDeleteChunkMap.find(id);
    if (it != mDeleteChunkMap.end())
    {
        return it->second.get();
    }

    mCurrentDeleteChunk.reset(new Chunk(mRoot, id));
    assert(mCurrentDeleteChunk.get());
    mDeleteChunkMap.insert(std::make_pair(id, mCurrentDeleteChunk));
    return mCurrentDeleteChunk.get();
}

bool PanguAppendStore::CreateDirectory(const std::string& name)
{
    try
    {
        mFileSystemPtr->CreateDirectory(name, CapabilityGenerator::Generate(std::string("pangu://"),PERMISSION_ALL));
    }
    catch (DirectoryExistException& e)
    {
        return false;
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("error on CreateDirectory", e.ToString()));
        throw;
    }
    return true;
}

bool PanguAppendStore::CheckDirs(const std::string& root)
{
    std::string index_path = root + std::string(Defaults::IDX_DIR);
    std::string data_path = root+ std::string(Defaults::DAT_DIR);
    std::string log_path = root + std::string(Defaults::LOG_DIR);

    if (PanguHelper::IsDirectoryExist(index_path.c_str()) 
        && PanguHelper::IsDirectoryExist(data_path.c_str())
        && PanguHelper::IsDirectoryExist(log_path.c_str()) )
    {
        return true;
    }
    return false;
}

void PanguAppendStore::CreateDirs(const std::string& root)
{
    //create root path
    if (CreateDirectory(root))
    {
        LOG_DEBUG(sLogger, ("CreateDirectory", root));
    }

    //create index path
    std::string index_path = root+ std::string(AppendStore::Defaults::IDX_DIR);
    if (CreateDirectory(index_path.c_str()))
    {
        LOG_DEBUG(sLogger, ("CreateDirectory", index_path));
    }

    //create data path
    std::string data_path = root+ std::string(AppendStore::Defaults::DAT_DIR);
    if (CreateDirectory(data_path.c_str()))
    {
        LOG_DEBUG(sLogger, ("CreateDirectory", data_path));
    }

    //create delete log path
    std::string log_path = root + std::string(AppendStore::Defaults::LOG_DIR);
    if (CreateDirectory(log_path.c_str()))
    {
        LOG_DEBUG(sLogger, ("CreateDirectory", log_path));
    }
}


PanguScanner::PanguScanner(const std::string& path, const DataFileCompressionFlag cflag)
    : mRoot(path), mCompressionFlag(cflag)
{
    if (mRoot.compare(mRoot.size()-1, 1, "/"))
    {
        mRoot.append("/");
    }

    InitScanner();
}

void PanguScanner::InitScanner()
{
    InitPangu();

    GetAllChunkID(mRoot);

    std::string compressAlgo(LzoCodec::mName);
    if (NO_COMPRESSION == mCompressionFlag) 
    {
        compressAlgo = NoneCodec::mName;
    }
    
    mScannerCodec.reset(CompressionCodec::getCodec(compressAlgo.c_str(), 1024, false));

    if (!mChunkList.empty())
    {
        mChunkId = mChunkList.front();
        mChunkList.pop_front();
        std::string fileName = Chunk::GetDatFname(mRoot, mChunkId);
        std::string logFileName = Chunk::GetLogFname(mRoot, mChunkId);
        mScannerFileStream = PanguHelper::OpenLog4Read(fileName);
        ReadDeleteLog(logFileName);
    }
}

void PanguScanner::ReadDeleteLog(const std::string& fname)
{
    mDeleteSet.clear();
    bool fexist = false;

    try 
    {
        fexist = PanguHelper::IsFileExist(fname);
    }
    catch (ExceptionBase & e)
    {
        LOG_ERROR(PanguAppendStore::sLogger, ("IsFileExist", fname));
        throw;
    }
    
    if (fexist) 
    {
        LogFileInputStreamPtr deleteLogFile = PanguHelper::OpenLog4Read(fname);
        try
        {
            do
            {
                uint32_t bufSize = deleteLogFile->GetNextLogSize();
                if (bufSize!=0)
                {
                    std::string buf="";
                    buf.resize(bufSize, '\0');
                    deleteLogFile->ReadLog(&buf[0], bufSize);
                    DeleteRecord r(buf);
                    mDeleteSet.insert(r.mIndex);
                }
                else
                {
                    break;
                }
            }
            while (1);
            deleteLogFile->Close();
        }
        catch (ExceptionBase & e)
        {
            LOG_ERROR(PanguAppendStore::sLogger, ("Error while reading deletelog", fname));
            throw;
        }
    }
}

PanguScanner::~PanguScanner()
{
    if (mScannerFileStream)
    {
        try
        {
            mScannerFileStream->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG_ERROR(PanguAppendStore::sLogger, ("Failed to close file for scanner", ex.ToString()));
        }
        mScannerFileStream.reset();
    }
    //UninitPangu();
}


void PanguScanner::GetAllChunkID(const std::string& root)
{
    std::string data_root = root + Defaults::DAT_DIR;

    // scan all files from the AppendStore data directory.
    std::vector<std::string> data_files;
    try
    { 
        FileSystem::GetInstance()->ListDirectory(data_root, "", DF_MAXFILENO, data_files);
    }
    catch (ExceptionBase& e)
    {
        APSARA_THROW(AppendStoreReadException, e.ToString()+"Cannot open list directory");
    }

    std::vector<std::string>::const_reverse_iterator it = data_files.rbegin();
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
            mChunkList.push_back(tmp_id);
        }
    }
    std::sort(mChunkList.begin(), mChunkList.end()); 
}

bool PanguScanner::Next(std::string* handle, std::string* item) 
{
    try
    {
        do
        {
            if ((mDataStream.peek()==EOF)||(mDataStream.eof()))
            {
                if (mDataStream.bad())
                {
                    LOG_ERROR(PanguAppendStore::sLogger, ("Error geting next", "Bad data stream"));
                }
                    
                uint32_t bufSize = mScannerFileStream->GetNextLogSize();
                if (bufSize > 0)
                {
                    std::string buf;
                    buf.resize(bufSize);
                    uint32_t rsize = mScannerFileStream->ReadLog(&buf[0], bufSize);
                    if (rsize != bufSize)
                    {
                        LOG_ERROR(PanguAppendStore::sLogger, ("Error", "file read error in Scanner"));
                        APSARA_THROW(AppendStoreReadException, "file read error in Scanner");
                    }

                    std::stringstream ss(buf);
                    CompressedDataRecord crd;
                    crd.Deserialize(ss);

                    uint32_t uncompressedSize;
                    buf.resize(crd.mOrigLength);
                    int retc = mScannerCodec->decompress(&(crd.mData[0]), crd.mCompressLength, &buf[0], uncompressedSize);
                    if (uncompressedSize != crd.mOrigLength)
                    {
                        LOG_ERROR(PanguAppendStore::sLogger, ("Error", "error when decompressing due to invalid length"));
                        APSARA_THROW(AppendStoreCompressionException, "decompression invalid length");
                    }
                    if (retc < 0)
                    {
                        LOG_ERROR(PanguAppendStore::sLogger, ("Error", "decompression codec error when decompressing inside Next()"));
                        APSARA_THROW(AppendStoreCompressionException, "decompression codec error");
                    }

                    mDataStream.str(buf);
                    mDataStream.clear();  
                }
                else if (mChunkList.empty())
                {
                     return false;
                }
                else 
                {
                    mChunkId = mChunkList.front();
                    std::string fileName = Chunk::GetDatFname(mRoot, mChunkList.front());
                    mChunkList.pop_front();
                    mScannerFileStream->Close();

                    mScannerFileStream = PanguHelper::OpenLog4Read(fileName);
                    std::string logFileName = Chunk::GetLogFname(mRoot, mChunkList.front());
                    ReadDeleteLog(logFileName);
                    continue;
                 }
             }

             DataRecord r;
             r.Deserialize(mDataStream);
             if (mDeleteSet.find(r.mIndex) != mDeleteSet.end())
             {
                 continue;
             }
             Handle h(mChunkId, r.mIndex);
             *handle = h.ToString();
             *item = r.mVal;
             return true;
        } while(1);
    }
    catch (ExceptionBase& e)
    {
        LOG_ERROR(PanguAppendStore::sLogger, ("Error while geting next", mRoot));
        throw;
    }

    return false;
}

