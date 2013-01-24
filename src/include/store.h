#ifndef _STORE_INTERFACE_H
#define _STORE_INTERFACE_H

#include <string>
#include <vector>
#include <stdint.h>

const char panguStore[] = "PanguAppendStore";

// typedef unsigned long long int uint64_t;
// typedef unsigned long int uint32_t;


class Scanner 
{
public: 
    virtual ~Scanner() {};

    virtual bool Next(std::string* handle, std::string* item) = 0;
};      


class Store
{
public:
    virtual ~Store() {};

    /**
     * \brief append a string to the data store
     * @param data:   the input data
     * @return handle as a string, which can be used to retrieve the data later
     * 
     *  Todo: remove
     */
    virtual std::string Append(const std::string& data) = 0;

    /**
     * \brief Bath mode: append a string to the data store
     * input param:  datavec,   the input data vector
     * output param: handlevec, hold vector of handle corresponding to each data element. 
     *        
     *        Each handle is a string, and can be used to retrieve data later.
     *        Make sure handlevec is empty when passing in.
     * @throw exception on error.
     */
    virtual void BatchAppend(const std::vector<std::string>& datavec, std::vector<std::string>& handlevec) = 0;

    /**
     * \brief read a string referred by handle h
     * @param h string handle of the data
     * @param data string buffer to hold result
     * @return true if found, false if data not found.
     */
    virtual bool Read(const std::string& h, std::string* data) = 0;

    /**
     * \brief remove a data block referred by handle
     * @param h handle of the data
     */
    virtual void Remove(const std::string& h) = 0;

    /**
     * \brief flush data to disk
     *  
     *  Todo: remove
     */
    virtual void Flush() = 0;

    /**
     * \brief ask reader to refresh the index files
     * @throw exception on error.
     */
    virtual void Reload() = 0;

    /**
     * force release data index hold in memory.
     */
    virtual void GarbageCollection(bool force) = 0;

    virtual Scanner* GetScanner() = 0;
};

enum DataFileCompressionFlag
{
    NO_COMPRESSION            = 0x0,
    COMPRESSOR_LZO            = 0x1
};

class StoreParameter
{
public:
    StoreParameter() 
      : mMaxChunkSize(0), mAppend(false), mBlockIndexInterval(1000), mCompressionFlag(COMPRESSOR_LZO) {};

    std::string mPath;
    uint64_t    mMaxChunkSize;
    bool        mPangu;
    bool        mAppend;
    uint32_t    mBlockIndexInterval;
    DataFileCompressionFlag  mCompressionFlag;
};

class StoreFactory 
{
public:
    virtual ~StoreFactory() {};

    /*
    * create a store writer. 
    * mAppend in StoreParameter is true
    */
    static Store* Create(const StoreParameter&, const std::string&);

    /* 
    * create a store reader. 
    * mAppend in StoreParameter is false
    */ 
    static Store* Load(const StoreParameter&, const std::string&); 
};


/*
class StoreUtility
{
public:
    
     * Make sure: InitPangu() is called somewhere before using this function!
     *
     * get the size of apend store.
     * return:
     *   0 if the path does not exist. 
     *   size of store (before pangu meta update).
     * throw exception on error.
    
    static uint64_t GetSize(const std::string& rootPath);

private:    
    static uint64_t GetDirectorySize(const std::string& dirName);
};
*/

#endif
