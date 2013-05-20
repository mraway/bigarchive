#ifndef _APPEND_STORE_H
#define _APPEND_STORE_H

#include <log4cxx/logger.h>
#include "../include/store.h"
#include "../include/exception.h"
#include "append_store_types.h"
#include "append_store_chunk.h"
#include "CompressionCodec.h"

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
    // QFS doesn't allow concurrent read/write to the same QFS chunk,
    // so we have to turn on/off reader and writer permission when needed,
    // read happens when we call Chunk.Read(), write happens when we call Chunk.Append() or Chunk.Flush(),
    // turn on read will disable write to the same chunk, and vice versa.
    void TurnOnWrite(Chunk* writer);
    void TurnOnRead(Chunk* reader);

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
    static LoggerPtr logger_;
};


#endif
