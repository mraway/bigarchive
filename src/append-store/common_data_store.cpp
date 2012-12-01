#include <iostream>
#include <fstream>
#include "common_data_store.h"
#include "protocol/cds_cj.h"
#include "apsara/pangu.h"
#include "apsara/common/serialize.h"
#include "apsara/pgfstream.h"

using namespace std;
using namespace apsara;
using namespace apsara::pangu;
using namespace apsara::security;
using namespace apsara::AppendStore;



namespace 
{
    const std::string DataFileSuffix  = "CDS.dat";
    const std::string IndexFileSuffix = "CDS.idx";

    const int DF_MINCOPY = 3;
    const int DF_MAXCOPY = 5;

    const uint32_t IndexInterval = 1000;

    const int OffsetSize = 8;

    const char AppName[]  = BIGFILE_APPNAME;
    const char PartName[] = BIGFILE_PARTNAME;
}

apsara::logging::Logger* CDSUtility::sLogger = apsara::logging::GetLogger("/apsara/cds_store");

apsara::logging::Logger* CdsIndexReader::sLoggerReader = apsara::logging::GetLogger("/apsara/cds_reader");


CDSUtility::CDSUtility(const std::string& panguPath, bool write)
    : mPath(panguPath), 
      mAppend(write),
      mFlushCount(0),
      mIndexInterval(IndexInterval)
{
    if (mPath.compare(mPath.size()-1, 1, "/"))
    {
        mPath.append("/");
    }

    mDataFileName  = mPath + DataFileSuffix;
    mIndexFileName = mPath + IndexFileSuffix;

    Init();
}

void CDSUtility::Close()
{
    try
    {
        Flush();
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in ~CDSUtility() Flush: ", e.ToString()));
    }
    try
    {
        if (mDataInputStream)
        {
            mDataInputStream->Close();
        }
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in Stream Close(): ", e.ToString()));
    }
    try
    {
        if (mDataOutputStream)
        {
            mDataOutputStream->Close();
        }
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in Stream Close(): ", e.ToString()));
    }
    try
    {
        if (mIndexOutputStream)
        {
            mIndexOutputStream->Close();
        }
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in Stream Close(): ", e.ToString()));
    }
}

CDSUtility::~CDSUtility()
{
    Close();
    //UninitPangu();
}

void CDSUtility::Init()
{
    InitPangu();
    mFileSystemPtr = FileSystem::GetInstance();

    bool direxist;
    try
    {
        direxist = AppendStore::PanguHelper::IsDirectoryExist(mPath);
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in Init() Directory: ", e.ToString()));
        throw;
    }

    if (mAppend) 
    {
        if (!direxist)
        {
            try
            {
                mFileSystemPtr->CreateDirectory(mPath, CapabilityGenerator::Generate(std::string("pangu://"),PERMISSION_ALL));
            }
            catch(ExceptionBase& e)
            {
                LOG_ERROR(sLogger, ("Error in Init() CreateDirectory: ", e.ToString()));
                throw;
            }
        }
    }
    else if (!direxist)
    {
        LOG_ERROR(sLogger, ("Error: ", "Directory not exist and readonly"));
        APSARA_THROW(ExceptionBase, "CDS store Directory not exist and readonly");
    }

    bool dexist;
    bool iexist;
    try
    {
        dexist = AppendStore::PanguHelper::IsFileExist(mDataFileName); 
        iexist = AppendStore::PanguHelper::IsFileExist(mIndexFileName); 
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in Init() File: ", e.ToString()));
        throw;
    }
    if (! ((dexist && iexist) || (!dexist && !iexist)) )
    {
        LOG_ERROR(sLogger, ("Error: ", "mDataFileName and mIndexFileName not co-exist"));
        APSARA_THROW(ExceptionBase, "mDataFileName and mIndexFileName not co-exist");
    }

    if (mAppend) 
    {
        if (!dexist && !iexist)
        {
            try
            {
                AppendStore::PanguHelper::CreateLogFile(mDataFileName, DF_MINCOPY, DF_MAXCOPY, AppName, PartName);
                AppendStore::PanguHelper::CreateLogFile(mIndexFileName, DF_MINCOPY, DF_MAXCOPY, AppName, PartName);
            }
            catch(ExceptionBase& e)
            {
                LOG_ERROR(sLogger, ("Error in Init() CreateLogFile: ", e.ToString()));
                throw;
            }
        }
        mDataOutputStream  = PanguHelper::OpenLog4Append(mDataFileName);
        mIndexOutputStream = PanguHelper::OpenLog4Append(mIndexFileName);
    }
    else 
    {
        mDataInputStream = PanguHelper::OpenLog4Read(mDataFileName);
    }
}

bool CDSUtility::Read(const std::string& handle, std::string* data) 
{
    // handle -> offset;
    uint64_t offset;
    memcpy(&offset, &handle[0], OffsetSize);

    try
    {
        mDataInputStream->Seek(offset); 
        uint32_t read_len = mDataInputStream->GetNextLogSize();
        
        data->clear();
        data->resize(read_len, '\0');
        uint32_t size = mDataInputStream->ReadLog(&((*data)[0]), read_len);

        if (read_len != size)
        {
            std::stringstream ss;
            ss << "DataInputStream file read error, need size: " << size << " actual size: "<<read_len;
            APSARA_THROW(ExceptionBase, ss.str());
        }
    }
    catch (ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in Read(): ", e.ToString()));
        throw;
    }

    return true;
}

// append data to DataFile
// aggregate index, and append to IndexFile 
std::string CDSUtility::Append(const std::string index, const std::string& data)
{
    if (!mAppend)
    {
        APSARA_THROW(ExceptionBase, "Cannot append to readOnly CDS store");
    }

    assert(index.size() == 20);

    uint64_t offset;
    try
    {
        offset = mDataOutputStream->FlushLog((char*)&data[0], data.size());
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error in FlushLog", e.ToString()));
        throw;
    }

    std::string ret;
    ret.resize(OffsetSize, '\0');
    memcpy(&ret[0], &offset, OffsetSize);

    CdsIndexRecord ci(index, ret);
    ci.Serialize(mIndexStream);
    ++mFlushCount;
    if (mFlushCount >= mIndexInterval)
    {
        AppendIndex();
    }

    return ret; 
}

void CDSUtility::AppendIndex()
{
    if (mFlushCount == 0) 
    {
        return;
    }

    std::stringstream ssbuf;
    MultiIndexRecord mir(mFlushCount, mIndexStream.str().size(), mIndexStream.str());
    mir.Serialize(ssbuf);
    const std::string& ssref = ssbuf.str();
    uint64_t fileSize = 0;

    try
    {
        fileSize = mIndexOutputStream->FlushLog((char*)&ssref[0], ssref.size());
    }
    catch (ExceptionBase& e)
    {
        LOG_ERROR(sLogger, ("Error", e.ToString()));
        throw;
    }
    LOG_DEBUG(sLogger, ("FlushCount", mFlushCount)("fileSize", fileSize));

    mFlushCount = 0;
    mIndexStream.str("");
    mIndexStream.clear();         
};

void CDSUtility::Flush()
{
    LOG_INFO(sLogger, ("Flush", mFlushCount));
    if (mFlushCount > 0)
    {
        AppendIndex();
    }
}

std::vector<std::string> CDSUtility::BatchAppend(const std::vector<std::string>& indexvec, const std::vector<std::string>& datavec)
{
    std::vector<std::string> retv;

    assert(indexvec.size() == datavec.size());
    uint32_t vsize = datavec.size();
    if (vsize > 0)
    {
        for (uint32_t i=0; i<vsize; ++i)
        {
            retv.push_back(Append(indexvec[i], datavec[i]));
        }
        Flush();
    }

    return retv;
}


CdsIndexRecord::CdsIndexRecord() 
{ 
}

CdsIndexRecord::CdsIndexRecord(const std::string& i, const std::string& s)
    :mSha1Index(i), mOffset(s) 
{
}

CdsIndexRecord* CdsIndexRecord::New()
{
    return new CdsIndexRecord();
}

void CdsIndexRecord::Serialize(std::ostream& os) const
{
    apsara::Serialize(mSha1Index, os);
    apsara::Serialize(mOffset, os);
}

void CdsIndexRecord::Deserialize(std::istream& is)
{
    apsara::Deserialize(mSha1Index, is);
    apsara::Deserialize(mOffset, is);
}

void CdsIndexRecord::Copy(const Serializable& rec)
{
    const CdsIndexRecord& tmpRec = dynamic_cast<const CdsIndexRecord&>(rec);
    mSha1Index = tmpRec.mSha1Index;
    mOffset    = tmpRec.mOffset;
}

uint32_t CdsIndexRecord::Size() const
{
    return sizeof(mSha1Index)+sizeof(mOffset);
}


MultiIndexRecord::MultiIndexRecord() 
    : mRecords(0),
      mLength(0),
      mData("")
{
}

MultiIndexRecord::MultiIndexRecord(const uint32_t& num, const uint32_t& len, const std::string& value)
    : mRecords(num),
      mLength(len),
      mData(value)
{ 
}

void MultiIndexRecord::Serialize(std::ostream& os) const
{
    apsara::Serialize(mRecords, os);
    apsara::Serialize(mLength, os);
    apsara::Serialize(mData, os);
}

MultiIndexRecord* MultiIndexRecord::New()
{
    return new MultiIndexRecord();
}

void MultiIndexRecord::Deserialize(std::istream& is)
{
    apsara::Deserialize(mRecords, is);
    apsara::Deserialize(mLength, is);
    apsara::Deserialize(mData, is);
}

void MultiIndexRecord::Copy(const Serializable& rec)
{
    const MultiIndexRecord& tmpRec = dynamic_cast<const MultiIndexRecord&>(rec);
    mRecords = tmpRec.mRecords;
    mLength  = tmpRec.mLength;
    mData    = tmpRec.mData;
}

void MultiIndexRecord::clear()
{
    mRecords = 0;
    mLength  = 0;
    mData.clear();
}


CdsIndexReader::CdsIndexReader(const std::string& path)
    : mPath(path), mPartition_id(-1)
{
    InitReader();
}
   
CdsIndexReader::CdsIndexReader(const std::string& path, uint32_t partition_id)
    : mPath(path), mPartition_id(partition_id)
{
    InitReader();
}

void CdsIndexReader::InitReader()
{
    if (mPath.compare(mPath.size()-1, 1, "/"))
    {
        mPath.append("/");
    }
    InitPangu();

    std::string IndexFileName = mPath + IndexFileSuffix;
    if (mPartition_id >= 0)
    {
        std::stringstream ss;
        ss << mPartition_id;
        IndexFileName.append("." + ss.str());
    }
    bool iexist;
    try
    {
        iexist = AppendStore::PanguHelper::IsFileExist(IndexFileName); 
    }
    catch(ExceptionBase& e)
    {
        LOG_ERROR(sLoggerReader, ("Error in IndexReader ctr: " + IndexFileName, e.ToString()));
        throw;
    }
    if (!iexist)
    {
        LOG_ERROR(sLoggerReader, ("Error: ", "IndexFileName not co-exist"));
        APSARA_THROW(ExceptionBase, "IndexFileName not co-exist");
    }

    mIndexInputStream = PanguHelper::OpenLog4Read(IndexFileName);
}

CdsIndexReader::~CdsIndexReader()
{
    if (mIndexInputStream)
    {
        mIndexInputStream->Close();
    }
    //UninitPangu();
}

bool CdsIndexReader::Next(MultiIndexRecord& out_record)
{
    out_record.clear();

    try
    {
        uint32_t indexSize = mIndexInputStream->GetNextLogSize();
        if (indexSize > 0)
        {
            std::string buffer;
            buffer.resize(indexSize, 0);
            uint32_t rsize = mIndexInputStream->ReadLog(&buffer[0], indexSize);
            if (rsize != indexSize)
            {
                LOG_ERROR(sLoggerReader, ("Error", "file read error in IndexReader"));
                APSARA_THROW(ExceptionBase, "file read error in IndexReader");
            }

            std::stringstream ss(buffer);
            out_record.Deserialize(ss);
            LOG_DEBUG(sLoggerReader, ("CDS indexSize = ", indexSize));

            return true;
        }
        else
        {
            return false;
        }
    }
    catch (apsara::ExceptionBase& e)
    {
        APSARA_THROW(ExceptionBase, "error in Next(): " + e.ToString());
    }
    return false;
}

bool CdsIndexReader::Next(std::vector<CdsIndexRecord>& outvec)
{
    outvec.clear();
    int dirty = 0;
    
    MultiIndexRecord mir;
    if (Next(mir)) 
    {
        std::stringstream streamBuf(mir.mData);
        do
        {
            if (streamBuf.peek() == EOF)
            {
                break;
            }
            if (streamBuf.eof())
            {
                break;
            }
            if (streamBuf.bad())
            {
                LOG_ERROR(sLoggerReader, ("Error in Next() due to stream bad", "."));
                APSARA_THROW(ExceptionBase, "error in Next()");
            }

            CdsIndexRecord cr;
            cr.Deserialize(streamBuf);
            outvec.push_back(cr);
            dirty = 1;
        } while (1);
    }
LOG_INFO(sLoggerReader, ("in Next()", outvec.size()));
    if (dirty) 
    {
        return true;
    }
    else
    {
        return false;
    }
}

uint32_t CdsIndexRecord::mod(const uint32_t no) const
{
    assert(mSha1Index.size() == 20);
    assert(no > 0);

    uint32_t hash_string[40];
    for (size_t i=0; i<20; ++i)
    {
        hash_string[2*i]   = static_cast<uint8_t>(mSha1Index[i]) / 16;
        hash_string[2*i+1] = static_cast<uint8_t>(mSha1Index[i]) % 16;
    }

    uint32_t mod = 0;
    for (size_t i=0; i<40; i++)
    {
        uint32_t digit = hash_string[i];
        mod = (mod*16 + digit) % no;
    }
    return mod;
}

bool apsara::AppendStore::GeneratePartitionIndex(std::string& path, uint32_t no_partitions, std::string dest_path)
{
    CdsIndexReader reader(path);

    std::string ipath = dest_path;
    if (ipath.compare(ipath.size()-1, 1, "/"))
    {
        ipath.append("/");
    }
    std::string filename = ipath + IndexFileSuffix;

    std::vector<std::string> ifilevec;
    for (uint32_t j=0; j<no_partitions; ++j)
    {
        std::stringstream ss;
        ss << j;
        ifilevec.push_back(filename + "." + ss.str());
    }

    if (no_partitions < 1)
    {
        std::cerr << "Error: " << "no_partitions has to be no less than 1." << std::endl;
        return false;
    }

    for (uint32_t j=0; j<no_partitions; ++j)
    {
        bool iexist; 
        try
        {
            iexist = AppendStore::PanguHelper::IsFileExist(ifilevec[j]); 
        }
        catch(ExceptionBase& e)
        {
            std::cerr << "Error: " << ifilevec[j] << " exist" << std::endl;
            return false;
        }
        if (iexist)
        {
            std::cerr << "Error: " << ifilevec[j] << " already exist" << std::endl;
            return false;
        }
    }

    bool direxist;
    try
    {
        direxist = AppendStore::PanguHelper::IsDirectoryExist(dest_path);
    }
    catch(ExceptionBase& e)
    {
        std::cerr << "Error: IsDirectoryExist " << dest_path << std::endl;
        return false;
    }
    if (!direxist)
    {
        try
        {
            FileSystem::GetInstance()->CreateDirectory(dest_path, CapabilityGenerator::Generate(std::string("pangu://"), PERMISSION_ALL));
        }
        catch(ExceptionBase& e)
        {
            std::cerr << "Cannot createDirectory " << dest_path << std::endl;
            return false;
        }
    }

    std::vector<LogFileOutputStreamPtr> istreamvec;
    for (uint32_t j=0; j<no_partitions; ++j)
    {
        try
        {
            AppendStore::PanguHelper::CreateLogFile(ifilevec[j], DF_MINCOPY, DF_MAXCOPY, AppName, PartName);
        } 
        catch(ExceptionBase& e)
        {
            std::cerr << "Error in CreateLogFile: " << e.ToString() << std::endl;
            return false;
        }
        try
        {
            istreamvec.push_back(PanguHelper::OpenLog4Append(ifilevec[j]));
        } 
        catch(ExceptionBase& e)
        {
            std::cerr << "Error in CreateLogFile(" << j << "): " << e.ToString() << std::endl;
            return false;
        }   
    }

    std::vector<uint32_t> flushcountvec;
    std::vector<std::stringstream*> streamvec;
    for (uint32_t j=0; j<no_partitions; ++j)
    {
        flushcountvec.push_back(0);
        streamvec.push_back(new std::stringstream());
    }

    MultiIndexRecord record;
    while (reader.Next(record)) 
    {
        std::stringstream streamBuf(record.mData);
        do
        {
            if (streamBuf.peek() == EOF)
            {
                break;
            }
            if (streamBuf.eof()) 
            {
                break;
            }
            if (streamBuf.bad()) 
            {
                APSARA_THROW(ExceptionBase, "streamBuf.bad() in GeneratePartitionIndex()");
            }

            CdsIndexRecord cr;
            cr.Deserialize(streamBuf);
            uint32_t ith = cr.mod(no_partitions);

            cr.Serialize(*(streamvec[ith]));
            ++flushcountvec[ith];
            if (flushcountvec[ith] >= IndexInterval)
            {
                std::stringstream ssbuf;
                MultiIndexRecord mir(flushcountvec[ith], streamvec[ith]->str().size(), streamvec[ith]->str());
                mir.Serialize(ssbuf);
                const std::string& ssref = ssbuf.str();

                try
                {
                    istreamvec[ith]->FlushLog((char*)&ssref[0], ssref.size());
                }
                catch (ExceptionBase& e)
                {
                    std::cout << "Exception on FlushLog(" << ith << "): " << e.ToString() << std::endl;
                    return false;
                }

                flushcountvec[ith]= 0;
                streamvec[ith]->str(std::string());
                streamvec[ith]->clear();
            }
        } while (1);
    }

    for (uint32_t j=0; j<no_partitions; ++j)
    {
        if (flushcountvec[j] > 0)
        {
            std::stringstream ssbuf;
            MultiIndexRecord mir(flushcountvec[j], streamvec[j]->str().size(), streamvec[j]->str());
            mir.Serialize(ssbuf);
            const std::string& ssref = ssbuf.str();

            try
            {
                istreamvec[j]->FlushLog((char*)&ssref[0], ssref.size());
            }
            catch (ExceptionBase& e)
            {
                std::cout << "Exception on FlushLog(" << j << "): " << e.ToString() << std::endl;
                return false;
            }

            flushcountvec[j] = 0;
            streamvec[j]->clear();
        }
    }

    for (uint32_t j=0; j<no_partitions; ++j)
    {
        delete(streamvec[j]);
        streamvec[j] = NULL;
        try
        {
            istreamvec[j]->Close();
        }
        catch (ExceptionBase& e)
        {
            std::cout << "Exception on Close(" << j << "): " << e.ToString() << std::endl;
            return false;
        }
    }

    return true;
}

