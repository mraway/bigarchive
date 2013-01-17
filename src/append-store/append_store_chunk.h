#ifndef	_ASCHUNK_H
#define	_ASCHUNK_H

#include <string>
#include <map>

#include "file_system_helper.h"
#include "file_helper.h"
#include "qfs_file_helper.h"
// #include "qfs_file_system_helper.h"
#include "append_store_types.h"
#include "append_store_index.h"
#include "CompressionCodec.h"
#include <stdio.h>

struct Defaults
{
    static const char* IDX_DIR;
    static const char* DAT_DIR;
    static const char* LOG_DIR;
};


class Chunk
{
public:
    Chunk(const std::string& root, ChunkIDType chunk_id, 
          uint64_t max_chunk_sz, bool append, CompressionCodecWeakPtr weakptr,  
          CacheWeakPtr cacheptr, uint32_t index_interval=DF_MAX_PENDING);

    Chunk(const std::string& root, ChunkIDType chunk_id);

    ~Chunk();
    
public:
    IndexType Append(const std::string& data);

    bool Read(IndexType idx, std::string* data);
    
    bool Remove(const IndexType& idx);
    
    ChunkIDType GetID() { return mChunkId; }
    
	// CHKIT
    static uint32_t GetChunkSize(const std::string& root, ChunkIDType chunk_id);


    static ChunkIDType GetMaxChunkID(const std::string& root);
    
    void Flush();

    bool IsChunkFull() const;

    friend class PanguScanner;
    
    bool Close();

private:
    std::string mRoot;		///< root path of the chunk
    ChunkIDType mChunkId;	///< chunk id
    std::string mIndexFileName;	///< pathname of the index file
    std::string mLogFileName;	///< pathname of the index log file

    std::string mDataFileName;	///< pathname of the data file
    std::auto_ptr<IndexVector> mIndexMap;  // in-memory map (with its disk image)

    IndexType  mMaxIndex;	///< max index allocated so far
    OffsetType mLastData;	///< offset of the end of the last piece of data
    uint64_t   mMaxChunkSize;	///< max chunk size (soft limit)
    uint32_t   mFlushCount;
    bool       mDirty;
    CompressionCodecWeakPtr mChunkCodec;
    CacheWeakPtr            mCachePtr;
    uint32_t   mBlockIndexInterval;

    FileSystemHelper* mFileSystemHelper;
    FileHelper* mDataInputFH;
    FileHelper* mDataOutputFH;
    FileHelper* mIndexOutputFH;
    FileHelper* mDeleteLogFH;
    // CHKIT
    std::stringstream mBlockStream;

    static const uint32_t OFF_MASK = 0x7fffffff;

    static const uint32_t FLAG_MASK = (1<<31);

private:	

    void CheckIfNew();

    IndexType GenerateIndex();

    void AppendIndex();

    void LoadDeleteLog();

    bool LoadIndex();
    
    bool LoadData(bool flag);
    
   // bool Close();

    bool ReadRaw(const OffsetType&  offset_mix, std::string& data) ;
    
    OffsetType AppendRaw(const IndexType& index, const uint32_t numentry, const std::string& data);

    bool ExtractDataFromBlock(const std::string& buf, IndexType index, std::string* data);

    static bool IsValid(const IndexType& value);
    
    static std::string GetIdxFname(const std::string& root, uint32_t chunk_id);

    static std::string GetIdxLogFname(const std::string& root, uint32_t chunk_id);

    static std::string GetDatFname(const std::string& root, uint32_t chunk_id);

    static std::string GetLogFname(const std::string& root, uint32_t chunk_id);
};


#endif //_ASCHUNK_H
