#include "append_store_scanner.h"

LoggerPtr AppendStoreScanner::logger_ = Logger::getLogger( "AppendStoreScanner");

AppendStoreScanner::AppendStoreScanner(const std::string& path, const DataFileCompressionFlag cflag)
    : mRoot(path), mCompressionFlag(cflag)
{
    if (mRoot.compare(mRoot.size()-1, 1, "/"))
    {
        mRoot.append("/");
    }

    InitScanner();
}

void AppendStoreScanner::InitScanner()
{
    // CHKIT
    // InitPangu();

    mFileSystemHelper = FileSystemHelper::GetInstance();
    GetAllChunkID(mRoot);


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
        mScannerFH = mFileSystemHelper->CreateFileHelper(fileName, O_RDONLY); 
        mScannerFH->Open();
        ReadDeleteLog(logFileName);
    }
}

void AppendStoreScanner::ReadDeleteLog(const std::string& fname)
{
    mDeleteSet.clear();
    bool fexist = false;

    try 
    {
        fexist = mFileSystemHelper->IsFileExists(fname);
    }
    catch (ExceptionBase & e)
    {
        LOG4CXX_ERROR(logger_, "IsFileExist : " << fname);
        throw;
    }
    
    if (fexist) 
    {
        FileHelper* deleteLogFH = mFileSystemHelper->CreateFileHelper(fname, O_RDONLY); 
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
            LOG4CXX_ERROR(logger_, "Error while reading deletelog : " << fname);
            throw;
        }
        mFileSystemHelper->DestroyFileHelper(deleteLogFH);
    }
}

AppendStoreScanner::~AppendStoreScanner()
{
    if (mScannerFH)
    {
        try
        {
            mScannerFH->Close();
            mFileSystemHelper->DestroyFileHelper(mScannerFH);
        }
        catch(ExceptionBase& ex)
        {
            LOG4CXX_ERROR(logger_, "Failed to close file for scanner : " << ex.ToString());
        }
        //mScannerFH->Seek(0); 
    }
    //UninitPangu();
}


void AppendStoreScanner::GetAllChunkID(const std::string& root)
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

bool AppendStoreScanner::Next(std::string* handle, std::string* item) 
{
    try
    {
        do
        {
            if ((mDataStream.peek() == EOF) || (mDataStream.eof()))
            {
                if (mDataStream.bad())
                {
                    LOG4CXX_ERROR(logger_, "Error geting next : Bad data stream");
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
                      LOG4CXX_ERROR(logger_, "Error file read error in Scanner");
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
                        LOG4CXX_ERROR(logger_, "Error error when decompressing due to invalid length");
                        THROW_EXCEPTION(AppendStoreCompressionException, "decompression invalid length");
                    }
                    if (retc < 0)
                    {
                        LOG4CXX_ERROR(logger_, "Error decompression codec error when decompressing inside Next()");
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
                    mFileSystemHelper->DestroyFileHelper(mScannerFH);
                    mScannerFH = mFileSystemHelper->CreateFileHelper(fileName, O_RDONLY); 
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
        LOG4CXX_ERROR(logger_, "Error while geting next : " <<  mRoot);
        throw;
    }

    return false;
}

