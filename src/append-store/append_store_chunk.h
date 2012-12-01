#ifndef	_ASCHUNK_H
#define	_ASCHUNK_H

#include <string>
#include <map>

#include "append_store_types.h"
#include "pangu_helper.h"
#include "append_store_index.h"
#include "CompressionCodec.h"
//#include "AppendStoreException.h"


namespace apsara
{
namespace AppendStore
{

struct Defaults
{
    static const char* IDX_DIR;
    static const char* DAT_DIR;
    static const char* LOG_DIR;
};


class Chunk
{
public:
    /**
     * \brief constructor of a chunk for append or read
     * @param root the root path of this chunk
     * @param chunk_id the id of this chunk
     * @param max_chunk_sz the maximum chunk size in bytes
     */
    Chunk(const std::string& root, ChunkIDType chunk_id, 
          uint64_t max_chunk_sz, bool append, CompressionCodecWeakPtr weakptr,  
          CacheWeakPtr cacheptr, uint32_t index_interval=DF_MAX_PENDING);

    /**
    * constructor of a chunk for remove record only
    */
    Chunk(const std::string& root, ChunkIDType chunk_id);

    ~Chunk();
    
public:
    /**
     * \brief Append a data item to the data store.
     * @param 	data the data item to be added.
     * @param	flag the flag (i.e. property) associated with the data.
     * @return the index of the newly appended data item (always non-zero); 
     * if it fails, it returns 0 
     */
    IndexType Append(const std::string& data);

    /**
     * \brief read a data item from the data store
     * @param idx      the index of the data item
     * @param data     the buf to hold the read data
     * @return true if succeeds, false otherwise
     */
    bool Read(IndexType idx, std::string* data);
    
    /**
     * \brief remove a data item from the data store
     * @param idx the index of the data item
     * @return true if the data exist and successfully removed
     * 		   false otherwise
     */
    bool Remove(const IndexType& idx);
    
    ChunkIDType GetID() { return mChunkId; }
    
    static uint32_t GetChunkSize(const std::string& root, ChunkIDType chunk_id);

    static ChunkIDType GetMaxChunkID(const std::string& root);
    
    void Flush();

    bool IsChunkFull() const;

    friend class PanguScanner;


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

    apsara::pangu::FileSystem* mFileSystemPtr;
    apsara::pangu::LogFileInputStreamPtr mDataInputStream;
    apsara::pangu::LogFileOutputStreamPtr mDataOutputStream;
    apsara::pangu::LogFileOutputStreamPtr mIndexOutputStream;
    apsara::pangu::LogFileOutputStreamPtr mDeleteLogStream;
    std::stringstream mBlockStream;
    
    static apsara::logging::Logger* sLogger;

    static const uint32_t OFF_MASK = 0x7fffffff;

    static const uint32_t FLAG_MASK = (1<<31);

private:	
    ///generate an index key based on current max_index_

    void CheckIfNew();

    IndexType GenerateIndex();

    void AppendIndex();

    void LoadDeleteLog();

    ///load the index file to an in-memory map
    bool LoadIndex();
    
    ///load data file
    bool LoadData(bool flag);
    
    ///close chunk, and dump all in-memory structure to disk
    bool Close();

    /* read a blob from a file
    *
    *
    */
    bool ReadRaw(const OffsetType&  offset_mix, std::string& data) ;
    
    /* write a blob
    *  index:   the index number
    *  numentry: the number of records
    */ 
    OffsetType AppendRaw(const IndexType& index, const uint32_t numentry, const std::string& data);

    bool ExtractDataFromBlock(const std::string& buf, AppendStore::IndexType index, std::string* data);

    static bool IsValid(const IndexType& value);
    
    static std::string GetIdxFname(const std::string& root, uint32_t chunk_id);

    static std::string GetIdxLogFname(const std::string& root, uint32_t chunk_id);

    static std::string GetDatFname(const std::string& root, uint32_t chunk_id);

    static std::string GetLogFname(const std::string& root, uint32_t chunk_id);
};

}
}

#endif//_ASCHUNK_H
