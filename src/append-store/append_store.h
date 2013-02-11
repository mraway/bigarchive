#ifndef _APPEND_STORE_H
#define _APPEND_STORE_H

#include <deque>
#include <set>
#include "../include/store.h"
#include "append_store_types.h"
#include "../include/exception.h"
#include "append_store_chunk.h"
#include "CompressionCodec.h"

/*
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// static logger variable
// LoggerPtr logger(Logger::getLogger( "appendstore"));
// typedef unsigned long long int uint64_t
*/

class PanguScanner : public Scanner 
{
public: 
    virtual ~PanguScanner();
    virtual bool Next(std::string* handle, std::string* item);

private:
    PanguScanner(const std::string& path, const DataFileCompressionFlag cflag);
    void InitScanner();
    void ReadDeleteLog(const std::string& fname);
    void GetAllChunkID(const std::string& root);
    friend class PanguAppendStore;

private:
    std::string                     mRoot;
    DataFileCompressionFlag         mCompressionFlag;
    mutable std::deque<ChunkIDType> mChunkList;
    mutable std::stringstream       mDataStream;
    std::auto_ptr<CompressionCodec> mScannerCodec;
    // CHKIT
    // mutable apsara::pangu::LogFileInputStreamPtr mScannerFileStream;
    mutable FileHelper* mScannerFH;
    // added file system helper !! CHKIT
    FileSystemHelper*   mFileSystemHelper;
    bool                mFileHasMore;
    mutable ChunkIDType mChunkId;
    std::set<IndexType> mDeleteSet;
};


class PanguAppendStore : public Store
{
public:
    PanguAppendStore(const StoreParameter& para, bool iscreate);
    virtual ~PanguAppendStore();
    virtual std::string Append(const std::string& data);
    virtual void BatchAppend(const std::vector<std::string>& datavec, std::vector<std::string>& handlevec);
    virtual bool Read(const std::string& h, std::string* data);
    virtual void Remove(const std::string& h);
    virtual void Flush();
    void Reload(); 
    virtual void GarbageCollection(bool force);  
    virtual Scanner* GetScanner();
    friend class PanguScanner;
    virtual void Close();

private:
    void Init(bool iscreate);
    bool ReadMetaInfo();
    void WriteMetaInfo(const std::string& root, const StoreMetaData&);
    void AllocNextChunk();
    Chunk* LoadAppendChunk();
    bool ValidChunkID(ChunkIDType id) const; 
    void CreateDirs(const std::string& root);
    bool CheckDirs(const std::string& root);
	// CHKIT CHKIT
    /* AppendStore:: */ Chunk* LoadRandomChunk(ChunkIDType id);
    /* AppendStore:: */ Chunk* LoadDeleteChunk(ChunkIDType id);
    bool CreateDirectory(const std::string&);

private:
    typedef std::tr1::shared_ptr<Chunk>     ChunkPtr;
    typedef std::map<ChunkIDType, ChunkPtr> ChunkMapType;
    std::string   mRoot;
    bool          mAppend;
    ChunkIDType   mMaxChunkId;
    ChunkIDType   mAppendChunkId;
    uint32_t      mCompressionType;
    StoreMetaData mMeta;
    CachePtr            mCache;
    CompressionCodecPtr mCodec;
    FileSystemHelper*   mFileSystemHelper;
    mutable std::auto_ptr<Chunk> mCurrentAppendChunk;  
    mutable ChunkPtr             mCurrentRandomChunk;  
    mutable ChunkPtr             mCurrentDeleteChunk;  
    ChunkMapType mChunkMap;          // for read map of chunk index 
    ChunkMapType mDeleteChunkMap;    // for read map of delete chunk index 
    // CHKIT
    // static apsara::logging::Logger* sLogger;
};


#endif
