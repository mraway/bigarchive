#ifndef _APPEND_STORE_SCANNER_H_
#define _APPEND_STORE_SCANNER_H_

#include <deque>
#include <set>
#include "../include/store.h"
#include "append_store_types.h"
#include "../include/exception.h"
#include "append_store_chunk.h"
#include <log4cxx/logger.h>

using namespace std;
using namespace log4cxx;

class AppendStoreScanner : public Scanner 
{
public: 
    virtual ~AppendStoreScanner();
    virtual bool Next(std::string* handle, std::string* item);

private:
    AppendStoreScanner(const std::string& path, const DataFileCompressionFlag cflag);
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
    static LoggerPtr logger_;
};
#endif










