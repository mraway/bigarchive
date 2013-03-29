#include <algorithm>
#include <set>
#include <map>
#include <string>

#include "append_store.h"
#include "../include/exception.h"
#include "append_store_scanner.h"

LoggerPtr PanguAppendStore::logger_ = Logger::getLogger( "AppendStore");

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
        mMeta.maxChunkSize = DF_CHUNK_SZ;
    }
    // if (para.mBlockIndexInterval == 0)
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
    return new AppendStoreScanner(mRoot, mMeta.compressionFlag);
}

std::string PanguAppendStore::Append(const std::string& data)
{
    if (!mAppend) {
        THROW_EXCEPTION(AppendStoreWriteException, "Cannot append for read-only store");
    }

    Chunk* p_chunk = LoadAppendChunk();
    Handle h;
    h.mIndex = p_chunk->Append(data);

    if (h.mIndex==0) {
        THROW_EXCEPTION(AppendStoreWriteException, "Wrong handle index ==0");
    }

    h.mChunkId = p_chunk->GetID();

    //LOG4CXX_INFO(logger_, "Store::Append " << mRoot << "mChunkId : " << p_chunk->GetID() << ", mIndex : " << h.mIndex << ", size : " << data.size());
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
        LOG4CXX_INFO(logger_, "Cache Hit in Store for Handle : " << handle.mChunkId << "," << handle.mIndex);
        return true;
    }
    
    Chunk* p_chunk = LoadRandomChunk(handle.mChunkId);

    if (p_chunk == 0)
    {
        return false;
    }

    bOK = p_chunk->Read(handle.mIndex, data);

    LOG4CXX_INFO(logger_, "Store::Read : " << mRoot << " & mChunkId : " << handle.mChunkId << " & mIndex : " <<  handle.mIndex);
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
    LOG4CXX_INFO(logger_, "Store::Removed : " << mRoot << " & mChunkId : " << handle.mChunkId << " & mIndex : " <<  handle.mIndex);
}

void PanguAppendStore::Close() {
	if(mAppend) {
		LOG4CXX_INFO(logger_, "In APPEND mode : Close last append chunk");
		Chunk* p_chunk = mCurrentAppendChunk.get();//LoadAppendChunk(); 
		if(p_chunk != 0) {
			p_chunk->Close();
		}
 	}
	
	std::map<ChunkIDType, ChunkPtr>::iterator chunk_iter;

	for (chunk_iter = mChunkMap.begin(); chunk_iter != mChunkMap.end(); chunk_iter++) {
		LOG4CXX_INFO(logger_, "Closing chunk " << chunk_iter->first);
		chunk_iter->second->Close();
	}
 
	for (chunk_iter = mDeleteChunkMap.begin(); chunk_iter != mDeleteChunkMap.end(); chunk_iter++) {
  		LOG4CXX_INFO(logger_, "Closing chunk " << chunk_iter->first);
  		chunk_iter->second->Close();
 	}
	
	LOG4CXX_INFO(logger_, "Store::Closed - All associated Chunks Closed");
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
    mFileSystemHelper = FileSystemHelper::GetInstance();
    
    if (iscreate) 
    {
        CreateDirs(mRoot);
    }

    uint32_t binmajor = mMeta.storemajor; 
    uint32_t binminor = mMeta.storeminor;

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
        LOG4CXX_DEBUG(logger_, "mMaxChunkSize : " << mMeta.maxChunkSize << " & mBlockIndexInterval : " << mMeta.blockIndexInterval);
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
        LOG4CXX_DEBUG(logger_, "meta file name " << metaFileName << " : " << fexist);
    }
    catch (ExceptionBase & e)
    {
        LOG4CXX_ERROR(logger_, "IsFileExist : " <<  metaFileName);
        throw;
    }

    if (fexist)
    {
        try
        {
	    	FileHelper* metaInputFH = mFileSystemHelper->CreateFileHelper(metaFileName, O_RDONLY);
			char *read_buffer = new char[sizeof(StoreMetaData)]; 
			metaInputFH->Open();
 		    metaInputFH->Read(read_buffer, sizeof(StoreMetaData));
		    metaInputFH->Close();
            mFileSystemHelper->DestroyFileHelper(metaInputFH);
		    mMeta.fromBuffer(read_buffer);
		    LOG4CXX_DEBUG(logger_, "after reading mMeta values : " << mMeta.storeminor << 
                          "," << mMeta.storemajor << 
                          "," << mMeta.maxChunkSize <<
                          "," << mMeta.blockIndexInterval << 
                          "," << mMeta.compressionFlag);
            return true;
        }
        catch (ExceptionBase& e)
        {
            THROW_EXCEPTION(AppendStoreWriteException, "Cannot open meta file for append " + e.ToString());
        }
    }
    LOG4CXX_INFO(logger_, "Store::ReadMetaDataInfo()" );
    return false;
}

void PanguAppendStore::WriteMetaInfo(const std::string& root, const StoreMetaData& meta)
{
    std::string metaFileName = root + MetaFileName;
    try
    {	
		FileHelper* metaOutputFH = mFileSystemHelper->CreateFileHelper(metaFileName, O_WRONLY);
		metaOutputFH->Open();
        char *write_buffer = new char[sizeof(StoreMetaData)]; 
	 	/* Copying into buffer from StoreMetaData */
        LOG4CXX_DEBUG(logger_, "before reading mMeta values : " << mMeta.storeminor << 
                      "," << mMeta.storemajor << 
                      "," << mMeta.maxChunkSize <<
                      "," << mMeta.blockIndexInterval << 
                      "," << mMeta.compressionFlag);
	 
		meta.toBuffer(write_buffer);
	 	metaOutputFH->WriteData(write_buffer, sizeof(StoreMetaData));
        metaOutputFH->Close();	
        mFileSystemHelper->DestroyFileHelper(metaOutputFH);
    }
    catch (ExceptionBase& e) 
    {
        THROW_EXCEPTION(AppendStoreWriteException, e.ToString()+" Cannot generate .meta_ file");
    }
	LOG4CXX_INFO(logger_, "Store::WroteMetaDataInfo()" );
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
            //LOG4CXX_DEBUG(logger_, "Loaded Current chunk");
            return mCurrentAppendChunk.get();
        }
        else
        {
            /* Close previous chunk and allocate new chunk */
            Chunk* p_chunk = mCurrentAppendChunk.get();
            p_chunk->Close();
	        LOG4CXX_DEBUG(logger_, "Allocating next chunk, because current chunk is Full");	
            AllocNextChunk();
        }
    }
    try
    {
        mCurrentAppendChunk.reset(new Chunk(mRoot, mAppendChunkId, mMeta.maxChunkSize, true, mCodec, mCache, mMeta.blockIndexInterval));
    }
    catch (...)
    {
        LOG4CXX_ERROR(logger_, "[AppendStore] chunk is disabled for append, chunk id : " << mAppendChunkId);
        throw;
    }

    if (0 == mCurrentAppendChunk.get())
    {
        THROW_EXCEPTION(AppendStoreWriteException, "Cannot get valid chunk for append");
    }
    LOG4CXX_INFO(logger_, "Store::LoadedAppendChunk" ); 
    //cout << endl << "Time Store::LoadAppendChunk() : " << t.stop() << " ms";
    return mCurrentAppendChunk.get();
}

Chunk* PanguAppendStore::LoadRandomChunk(ChunkIDType id) 
{
    if (ValidChunkID(id) == false)
    {
        return 0;
    }

    // first look at the current reading chunk
    if(mCurrentRandomChunk.get() != 0) {
    	if(mCurrentRandomChunk.get()->GetID() == id) {
            return mCurrentRandomChunk.get();
        }
    }

    // then search the opened chunks
    ChunkMapType::const_iterator it = mChunkMap.find(id);
    if (it != mChunkMap.end())
    {
        return it->second.get();
    }

    // still not found, have to open the chunk for read
    mCurrentRandomChunk.reset(new Chunk(mRoot, id, mMeta.maxChunkSize, false, mCodec, mCache));
    assert(mCurrentRandomChunk.get());
    mChunkMap.insert(std::make_pair(id, mCurrentRandomChunk));
    LOG4CXX_INFO(logger_, "Store::LoadedRandomChunk" );
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
	LOG4CXX_INFO(logger_, "Store::LoadedDeleteChunk" );
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
        LOG4CXX_ERROR(logger_, "error on CreateDirectory : " << e.ToString());
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

    if (CreateDirectory(root))
    {
        LOG4CXX_DEBUG(logger_, "CreateDirectory : " << root);
    }

    std::string index_path = root+ std::string(Defaults::IDX_DIR);
    if (CreateDirectory(index_path.c_str()))
    {
        LOG4CXX_DEBUG(logger_, "CreateDirectory : " << index_path);
    }

    std::string data_path = root+ std::string(Defaults::DAT_DIR);
    if (CreateDirectory(data_path.c_str()))
    {
        LOG4CXX_DEBUG(logger_, "CreateDirectory : " << data_path);
    }

    std::string log_path = root + std::string(Defaults::LOG_DIR);
    if (CreateDirectory(log_path.c_str()))
    {
        LOG4CXX_DEBUG(logger_, "CreateDirectory : " << log_path);
    }

	LOG4CXX_INFO(logger_, "Store::Directories Created" );
	
}

