#include <algorithm>
#include <set>
#include "append_store.h"
#include "exception.h"
#include <map>
// #include <iostream>
#include <string>

#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// static logger variable
LoggerPtr asimpl_logger(Logger::getLogger( "appendstore.impl"));

PanguAppendStore::PanguAppendStore(const StoreParameter& para, bool iscreate)
    : mRoot(para.mPath), 
     mAppend(para.mAppend), 
     mMaxChunkId(0), 
     mAppendChunkId(0),
     mCompressionType(para.mCompressionFlag)
{
    DOMConfigurator::configure("/home/prakash/log_config.xml");

    if (mRoot.compare(mRoot.size()-1, 1, "/"))
    {
        mRoot.append("/");
    }

    mMeta = StoreMetaData(MAJOR_VER, MINOR_VER, para.mMaxChunkSize, para.mBlockIndexInterval, para.mCompressionFlag);
    if (para.mMaxChunkSize == 0)
    {
        mMeta.maxChunkSize = DF_CHUNK_SZ;
    }
    if (para.mBlockIndexInterval == 0)
    {
        mMeta.blockIndexInterval = DF_MAX_PENDING;
    }
    Init(iscreate);
}

PanguAppendStore::~PanguAppendStore()
{
    
}

Scanner* PanguAppendStore::GetScanner()
{
    return new PanguScanner(mRoot, mMeta.compressionFlag);
}

std::string PanguAppendStore::Append(const std::string& data)
{
    if (!mAppend)
    {
        THROW_EXCEPTION(AppendStoreWriteException, "Cannot append for read-only store");
    }

    Chunk* p_chunk = LoadAppendChunk();
    Handle h;
    h.mIndex = p_chunk->Append(data);
    LOG4CXX_DEBUG(asimpl_logger, "Write " << mRoot << "mChunkId : " << p_chunk->GetID() << ", mIndex : " << h.mIndex << ", size : " << data.size());

    if (h.mIndex==0)
    {
        THROW_EXCEPTION(AppendStoreWriteException, "Wrong handle index ==0");
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

    LOG4CXX_DEBUG(asimpl_logger, "START READ : " << mRoot << " & mChunkId : " << handle.mChunkId << " & mIndex : " <<  handle.mIndex);
    
    Chunk* p_chunk = LoadRandomChunk(handle.mChunkId);

    if (p_chunk == 0)
    {
        return false;
    }

    bOK = p_chunk->Read(handle.mIndex, data);
    LOG4CXX_DEBUG(asimpl_logger, "DONE READ : " << mRoot << " & mChunkId : " << handle.mChunkId << " & mIndex : " <<  handle.mIndex);

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

void PanguAppendStore::Close() {
 
 if(mAppend) {
  LOG4CXX_INFO(asimpl_logger, "In APPEND mode : Close last append chunk");
  Chunk* p_chunk = mCurrentAppendChunk.get();//LoadAppendChunk(); 
  if(p_chunk != 0)
   p_chunk->Close();
 }

 // for each( pair<ChunkIDType, ChunkPtr> c in mChunkMap ) {
 std::map<ChunkIDType, ChunkPtr>::iterator chunk_iter;


 for (chunk_iter = mChunkMap.begin(); chunk_iter != mChunkMap.end(); chunk_iter++) {
  LOG4CXX_INFO(asimpl_logger, "Closing chunk " << chunk_iter->first);
  chunk_iter->second->Close();
 }
 
 for (chunk_iter = mDeleteChunkMap.begin(); chunk_iter != mDeleteChunkMap.end(); chunk_iter++) {
  LOG4CXX_INFO(asimpl_logger, "Closing chunk " << chunk_iter->first);
  chunk_iter->second->Close();
 }

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
    mFileSystemHelper = new QFSHelper();
    mFileSystemHelper->Connect();//"host", 30000);
    
    // create the outer directory and index, data and log file directories.
    if (iscreate) 
    {
        CreateDirs(mRoot);
    }

    uint32_t binmajor = mMeta.storemajor; 
    uint32_t binminor = mMeta.storeminor;

    

    LOG4CXX_DEBUG(asimpl_logger, "directories are created !!! " );
    // LOG4CXX_DEBUG(asimpl_logger, "bin major and minor: major  " << binmajor << " minor" << binminor);

    if (ReadMetaInfo())
    {
        if (! mMeta.check(binmajor, binminor))
        {
            THROW_EXCEPTION(AppendStoreMisMatchedVerException, "meta version is not matched");
        }
        mCompressionType = mMeta.compressionFlag;
    }
    else
    {
        if (iscreate)
        {
            WriteMetaInfo(mRoot, mMeta);
        } 
        else 
	{
            THROW_EXCEPTION(AppendStoreNotExistException, "store not exist (.meta_)" + mRoot);
        }
    } 

    std::string compressAlgo(LzoCodec::mName);
    if (mCompressionType == (uint32_t)NO_COMPRESSION) 
    {
        compressAlgo = NoneCodec::mName;
    }
    else if (mCompressionType != (uint32_t)COMPRESSOR_LZO) 
    {
        THROW_EXCEPTION(AppendStoreCodecException, "unknown compression type");
    }

    // directory exist 
    if (!CheckDirs(mRoot))
    {
        THROW_EXCEPTION(AppendStoreNotExistException, "store not exist (3 dirs)" + mRoot);
    }
    mMaxChunkId = Chunk::GetMaxChunkID(mRoot);

    if (mAppend)
    {
        LOG4CXX_INFO(asimpl_logger, "mMaxChunkSize : " << mMeta.maxChunkSize << " & mBlockIndexInterval : " << mMeta.blockIndexInterval);
        // Currently, append at the last chunk // set mMaxChunkId: chunks are in [0, mMaxChunkId] inclusive
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
    std::string metaFileName = mRoot + MetaFileName;

    bool fexist = false; 
    try  
    {
        fexist = mFileSystemHelper->IsFileExists(metaFileName);
	LOG4CXX_DEBUG(asimpl_logger, "meta file name " << metaFileName << " : " << fexist);
    }
    catch (ExceptionBase & e)
    {
        LOG4CXX_ERROR(asimpl_logger, "IsFileExist : " <<  metaFileName);
        throw;
    }

    if (fexist)
    {
        try
        {
	    // CHKIT
	    FileHelper* metaInputFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, metaFileName, O_RDONLY);
            char *read_buffer = new char[sizeof(StoreMetaData)]; 
            // metaInputFH->GetNextLogSize();
 	    metaInputFH->Read(read_buffer, sizeof(StoreMetaData));
	    metaInputFH->Close();
	    
	    // LOG4CXX_DEBUG(asimpl_logger, "before reading from buffer : read_buffer to mMeta");
            mMeta.fromBuffer(read_buffer);

	    LOG4CXX_DEBUG(asimpl_logger, "after reading from buffer : mMeta values " << mMeta.storeminor);
	    LOG4CXX_DEBUG(asimpl_logger, "after reading from buffer : mMeta values " << mMeta.storemajor);
	    LOG4CXX_DEBUG(asimpl_logger, "after reading from buffer : mMeta values " << mMeta.maxChunkSize);
	    LOG4CXX_DEBUG(asimpl_logger, "after reading from buffer : mMeta values " << mMeta.blockIndexInterval);
	    LOG4CXX_DEBUG(asimpl_logger, "after reading from buffer : mMeta values " << mMeta.compressionFlag);

            return true;
        }
        catch (ExceptionBase& e)
        {
            THROW_EXCEPTION(AppendStoreWriteException, "Cannot open meta file for append " + e.ToString());
        }
    }
    return false;
}

void PanguAppendStore::WriteMetaInfo(const std::string& root, const StoreMetaData& meta)
{
    std::string metaFileName = root + MetaFileName;
    try
    {	
	 // CHKIT
	 FileHelper* metaOutputFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, metaFileName, O_WRONLY);
         char *write_buffer = new char[sizeof(StoreMetaData)]; 
	 /* Copying into buffer from StoreMetaData */

	    LOG4CXX_DEBUG(asimpl_logger, "before reading from buffer : mMeta values " << mMeta.storeminor);
	    LOG4CXX_DEBUG(asimpl_logger, "b4 reading from buffer : mMeta values " << mMeta.storemajor);
	    LOG4CXX_DEBUG(asimpl_logger, "b4 reading from buffer : mMeta values " << mMeta.maxChunkSize);
	    LOG4CXX_DEBUG(asimpl_logger, "b4 reading from buffer : mMeta values " << mMeta.blockIndexInterval);
	    LOG4CXX_DEBUG(asimpl_logger, "b4 reading from buffer : mMeta values " << mMeta.compressionFlag);
	 
	 meta.toBuffer(write_buffer);
	 metaOutputFH->WriteData(write_buffer, sizeof(StoreMetaData));
         metaOutputFH->Close();	
    }
    catch (ExceptionBase& e) 
    {
        THROW_EXCEPTION(AppendStoreWriteException, e.ToString()+" Cannot generate .meta_ file");
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
	    LOG4CXX_DEBUG(asimpl_logger, "Loaded Current chunk");
            return mCurrentAppendChunk.get();
        }
        else
        {
	 /* Close previous chunk and allocate new chunk */
	 Chunk* p_chunk = mCurrentAppendChunk.get();
	 p_chunk->Close();
	 // 
         LOG4CXX_DEBUG(asimpl_logger, "Allocating next chunk, because current chunk is Full");	
	 AllocNextChunk();
        }
    }
    try
    {
        mCurrentAppendChunk.reset(new Chunk(mRoot, mAppendChunkId, mMeta.maxChunkSize, true, mCodec, mCache, mMeta.blockIndexInterval));
    }
    catch (...)
    {
        LOG4CXX_ERROR(asimpl_logger, "[AppendStore] chunk is disabled for append, chunk id : " << mAppendChunkId);
        throw;
    }

    if (0 == mCurrentAppendChunk.get())
    {
        THROW_EXCEPTION(AppendStoreWriteException, "Cannot get valid chunk for append");
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
        THROW_EXCEPTION(AppendStoreReadException, "Trying to read a chunk that is being modified, abort...");
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
        mFileSystemHelper->CreateDirectory(name); 
    }
    catch (DirectoryExistException& e)
    {
        return false;
    }
    catch(ExceptionBase& e)
    {
        LOG4CXX_ERROR(asimpl_logger, "error on CreateDirectory : " << e.ToString());
        throw;
    }
    return true;
}

bool PanguAppendStore::CheckDirs(const std::string& root)
{
    std::string index_path = root + std::string(Defaults::IDX_DIR);
    std::string data_path = root+ std::string(Defaults::DAT_DIR);
    std::string log_path = root + std::string(Defaults::LOG_DIR);

    if (mFileSystemHelper->IsDirectoryExists(index_path) 
        && mFileSystemHelper->IsDirectoryExists(data_path)
        && mFileSystemHelper->IsDirectoryExists(log_path) )
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
        LOG4CXX_DEBUG(asimpl_logger, "CreateDirectory : " << root);
    }

    //create index path
    std::string index_path = root+ std::string(Defaults::IDX_DIR);
    if (CreateDirectory(index_path.c_str()))
    {
        LOG4CXX_DEBUG(asimpl_logger, "CreateDirectory : " << index_path);
    }

    //create data path
    std::string data_path = root+ std::string(Defaults::DAT_DIR);
    if (CreateDirectory(data_path.c_str()))
    {
        LOG4CXX_DEBUG(asimpl_logger, "CreateDirectory : " << data_path);
    }

    //create delete log path
    std::string log_path = root + std::string(Defaults::LOG_DIR);
    if (CreateDirectory(log_path.c_str()))
    {
        LOG4CXX_DEBUG(asimpl_logger, "CreateDirectory : " << log_path);
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
    // CHKIT
    // InitPangu();

    GetAllChunkID(mRoot);

    mFileSystemHelper = new QFSHelper();
    mFileSystemHelper->Connect();//"host", 30000);

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
        mScannerFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, fileName, O_RDONLY); 
	mScannerFH->Open();
        ReadDeleteLog(logFileName);
    }
}

void PanguScanner::ReadDeleteLog(const std::string& fname)
{
    mDeleteSet.clear();
    bool fexist = false;

    try 
    {
        fexist = mFileSystemHelper->IsFileExists(fname);
    }
    catch (ExceptionBase & e)
    {
        LOG4CXX_ERROR(asimpl_logger, "IsFileExist : " << fname);
        throw;
    }
    
    if (fexist) 
    {
 	FileHelper* deleteLogFH = new QFSFileHelper((QFSHelper *)mFileSystemHelper, fname, O_RDONLY); 
	deleteLogFH->Open();
        try
        {
            do
            {
                uint32_t bufSize = deleteLogFH->GetNextLogSize();
                if (bufSize!=0)
                {
                    std::string buf="";
                    buf.resize(bufSize, '\0');
                    deleteLogFH->Read(&buf[0], bufSize);
                    DeleteRecord r(buf);
                    mDeleteSet.insert(r.mIndex);
                }
                else
                {
                    break;
                }
            }
	    // CHKIT
            while (1);
            deleteLogFH->Close();
        }
        catch (ExceptionBase & e)
        {
            LOG4CXX_ERROR(asimpl_logger, "Error while reading deletelog : " << fname);
            throw;
        }
    }
}

PanguScanner::~PanguScanner()
{
    if (mScannerFH)
    {
        try
        {
            mScannerFH->Close();
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(asimpl_logger, "Failed to close file for scanner : " << ex.ToString());
        }
        mScannerFH->Seek(0); 
    }
    //UninitPangu();
}


void PanguScanner::GetAllChunkID(const std::string& root)
{
    std::string data_root = root + Defaults::DAT_DIR;
    std::vector<std::string> data_files;
    try
    { 
        mFileSystemHelper->ListDir(data_root, data_files);
    }
    catch (ExceptionBase& e)
    {
        THROW_EXCEPTION(AppendStoreReadException, e.ToString()+"Cannot open list directory");
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
            if ((mDataStream.peek() == EOF) || (mDataStream.eof()))
            {
                if (mDataStream.bad())
                {
                    LOG4CXX_ERROR(asimpl_logger, "Error geting next : Bad data stream");
                }
                    
		// CHKIT
                uint32_t bufSize = mScannerFH->GetNextLogSize();
                if (bufSize > 0)
                {
                    std::string buf;
                    buf.resize(bufSize);

		    // CHKIT - read is verified in file helpers !! 

		    mScannerFH->Read(&buf[0], bufSize);

		    /*
                    if (rsize != bufSize)
                    {
                        LOG4CXX_ERROR(asimpl_logger, "Error file read error in Scanner");
                        THROW_EXCEPTION(AppendStoreReadException, "file read error in Scanner");
                    }
		    */

                    std::stringstream ss(buf);
                    CompressedDataRecord crd;
                    crd.Deserialize(ss);

                    uint32_t uncompressedSize;
                    buf.resize(crd.mOrigLength);
                    int retc = mScannerCodec->decompress(&(crd.mData[0]), crd.mCompressLength, &buf[0], uncompressedSize);
                    if (uncompressedSize != crd.mOrigLength)
                    {
                        LOG4CXX_ERROR(asimpl_logger, "Error error when decompressing due to invalid length");
                        THROW_EXCEPTION(AppendStoreCompressionException, "decompression invalid length");
                    }
                    if (retc < 0)
                    {
                        LOG4CXX_ERROR(asimpl_logger, "Error decompression codec error when decompressing inside Next()");
                        THROW_EXCEPTION(AppendStoreCompressionException, "decompression codec error");
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
                    mScannerFH->Close();
                    mScannerFH = new QFSFileHelper((QFSHelper*)mFileSystemHelper, fileName, O_RDONLY); 
		    mScannerFH->Open();
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
        LOG4CXX_ERROR(asimpl_logger, "Error while geting next : " <<  mRoot);
        throw;
    }

    return false;
}

