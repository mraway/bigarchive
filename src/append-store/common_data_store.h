#ifndef COMMON_DATA_H
#define COMMON_DATA_H

#include <vector>
#include <string>
#include <deque>
#include <string>
#include <sstream>

#include "serialize.h"
#include "../include/file_helper.h"

class CdsIndexRecord : public marshall::Serializable
{
public:
    CdsIndexRecord();

    CdsIndexRecord(const std::string& i, const std::string& s);

    void Serialize(std::ostream& os) const;

    CdsIndexRecord* New();

    void Deserialize(std::istream& is);

    void Copy(const Serializable& rec);

    uint32_t Size() const;

    uint32_t mod(const uint32_t no) const;

    std::string mSha1Index;		// 20-byte string
    //uint64_t    mOffset;
    std::string mOffset;                // 8-byte string

private:
    FileSystemHelper* mFileSystemHelper;
};

/*
* record consisting of multiple CdsIndexRecord
*/
class MultiIndexRecord : public marshall::Serializable
{
public:
    MultiIndexRecord();

    MultiIndexRecord(const uint32_t& num, const uint32_t& len, const std::string& value);

    void Serialize(std::ostream& os) const;

    MultiIndexRecord* New();

    void Deserialize(std::istream& is);

    void Copy(const Serializable& rec);

    void clear();

    uint32_t    mRecords;       // the number of records 
    uint32_t    mLength;
    std::string mData;
};


/*
 * to read the CDS index file, and the partitioned index files
*/
class CdsIndexReader
{
public:
    /*
     * throw 
    */
    CdsIndexReader(const std::string& path);

    /*
     * throw 
    */
    CdsIndexReader(const std::string& path, uint32_t partition_id);

    ~CdsIndexReader();

    /*
     * throw 
    */
    bool Next(MultiIndexRecord&);

    /*
     * throw 
    */
    bool Next(std::vector<CdsIndexRecord>&);

	// CHKIT
    // static apsara::logging::Logger* sLoggerReader;

private:
    void InitReader();

private:
    std::string mPath;
    int         mPartition_id;
    FileSystemHelper* mFileSystemHelper;
    mutable FileHelper* mIndexInputFH;
};


/*
 * to split the CDS index file into multiple partitioned index files
*/
bool GeneratePartitionIndex(std::string& path, uint32_t no_partitions, std::string dest_path);


/* utility class to generate/update CDS
 * CDS files on pangu
*/
class CDSUtility 
{
public:
    /*
     * throw 
    */
    CDSUtility(const std::string& panguPath, bool write=false);

    ~CDSUtility();

    /**
     * \brief Bath mode: append data vector to the CDS data store
     * indexvec:         vector of the sha1 hash of data (assume it is 20-byte)
     * datavec:          vector of data you want to store
     * return vector:    vector of handler which can be used to read from CDS store 
     * @throw exception on error.
     */
    std::vector<std::string> BatchAppend(const std::vector<std::string>& indexvec, const std::vector<std::string>& datavec);

    /* 
     * read a string from CDS store
     * handle: handler used to retrieve the data
     * data:   the data you want to retrieve 
     * return return true if found, false if data not found.
    */
    bool Read(const std::string& handle, std::string* data);

    void Close();

private:
    void Init();

    void AppendIndex();

    void Flush();

    /* 
     * append a string to CDS store
     * data: the data you want to store;
     * index: the sha1 hash of data (assume it is 20-byte);
     * return handle as a string which is used to retrieve the data, 
     *        the handle is actually 64-bit offset in file.
    */
    std::string Append(const std::string index, const std::string& data);

private:
    std::string  mPath;
    bool         mAppend;
    uint32_t     mFlushCount;
    uint32_t     mIndexInterval;    
    std::string  mIndexFileName; 
    std::string  mDataFileName;  
    std::stringstream mIndexStream;

    FileSystemHelper* mFileSystemHelper;
    FileHelper* mDataInputFH; //     apsara::pangu::LogFileInputStreamPtr  mDataInputStream;
    FileHelper* mDataOutputFH; // apsara::pangu::LogFileOutputStreamPtr mDataOutputStream;
    FileHelper* mIndexOutputFH;

    friend class IndexReader;
};



#endif // COMMON_DATA_H
