#include "apsara/common/random.h"
#include "apsara/common/flag.h"
#include "pangu_helper.h"
#include "append_store_types.h"

using namespace apsara::pangu;
using namespace apsara::AppendStore;


DEFINE_FLAG_INT32(append_store_LogFile_RetryCount, "retry count for log file operation", 1);
DEFINE_FLAG_INT32(append_store_LogFile_MinSleepInterval, "the minimum sleep interval when an exception happend. (us)", 100 * 1000);
DEFINE_FLAG_INT32(append_store_LogFile_MaxSleepInterval, "the maximum sleep interval when an exception happend. (us)", 500 *  1000);


static apsara::DefaultRandom sRandom;

apsara::logging::Logger* sLogger = apsara::logging::GetLogger("/apsara/append_store");


int64_t PanguHelper::GetFileSize(const std::string& fname)
{
    InitPangu();	// has to be here since it is a static 

    apsara::pangu::FileMeta meta;
    try
    {
        apsara::pangu::FileSystem::GetInstance()->GetFileMeta(fname, meta);
    }
    catch (apsara::FileNotExistException& e)
    {
        return 0;
    }
    catch(apsara::ExceptionBase& e)
    {
        throw;
    }
    return meta.fileLength;

}

void PanguHelper::CreateFile(const std::string& fname, int min, 
    int max, const std::string& appname, const std::string& partname)
{
    apsara::pangu::FileSystem* fileSystemPtr = apsara::pangu::FileSystem::GetInstance();
    try
    {
        fileSystemPtr->CreateFile(fname,
                           min, max,
                           appname, partname,
                           apsara::security::CapabilityGenerator::Generate(std::string("pangu://"),apsara::security::PERMISSION_ALL));
    }
    catch(apsara::ExceptionBase& e)
    {
        throw;
    }
}

void PanguHelper::CreateLogFile(const std::string& fname, int min, 
    int max, const std::string& appname, const std::string& partname)
{
    apsara::pangu::FileSystem* fileSystemPtr = apsara::pangu::FileSystem::GetInstance();
    try
    {
        fileSystemPtr->CreateLogFile(fname,
                           min, max,
                           appname, partname,
                           apsara::security::CapabilityGenerator::Generate(std::string("pangu://"),apsara::security::PERMISSION_ALL));
    }
    catch(apsara::ExceptionBase& e)
    {
        throw;
    }
}

bool PanguHelper::IsFileExist(const std::string& fname)
{
    apsara::pangu::FileSystem* fileSystemPtr = apsara::pangu::FileSystem::GetInstance();
    try
    {
        return fileSystemPtr->IsFileExist(fname);
    }
    catch(apsara::ExceptionBase& e)
    {
        throw;
    }
}

bool PanguHelper::IsDirectoryExist(const std::string& dirname)
{
    apsara::pangu::FileSystem* fileSystemPtr = apsara::pangu::FileSystem::GetInstance();
    try
    {
        return fileSystemPtr->IsDirectoryExist(dirname);
    }
    catch(apsara::ExceptionBase& e)
    {
        throw;
    }
}

LogFileOutputStreamPtr PanguHelper::OpenLog4Append(const std::string& fileName)
{
    int32_t retryCount = 0;
    do
    { 
        try
        {
            LogFileOutputStreamPtr os = FileSystem::GetInstance()->OpenLog4Append(fileName);
            return os;
        }
        catch (const PanguFileLockException& e)
        {
            LOG_INFO(sLogger, ("PanguFileLockException", e.ToString())
                ("FileName", fileName)("RetryCount", retryCount));
            ReleaseLogFileLock(fileName);
        }
        catch (const ExceptionBase& e)
        {
            LOG_WARNING(sLogger, ("ExceptionBase", e.ToString())
                ("FileName", fileName)("RetryCount", retryCount));
            if (++retryCount <= INT32_FLAG(append_store_LogFile_RetryCount))
            {
                usleep(sRandom.Get(INT32_FLAG(append_store_LogFile_MinSleepInterval),
                    INT32_FLAG(append_store_LogFile_MaxSleepInterval)));
            }
        }
    } while (retryCount <= INT32_FLAG(append_store_LogFile_RetryCount));

    LOG_WARNING(sLogger, ("OpenLog4Append", "Fail")("FileName", fileName));
    APSARA_THROW(AppendStoreExceptionBase, "OpenLog4Append fail, fileName:" + fileName);
}

LogFileInputStreamPtr PanguHelper::OpenLog4Read(const std::string& fileName)
{
    int32_t retryCount = 0;
    do
    {
        try
        {
            LogFileInputStreamPtr is = FileSystem::GetInstance()->OpenLog4Read(fileName);
            return is;
        }
        catch (const PanguFileLockException& e)
        {
            LOG_INFO(sLogger, ("PanguFileLockException", e.ToString())
                ("FileName", fileName)("RetryCount", retryCount));
            ReleaseLogFileLock(fileName);
        }
        catch (const ExceptionBase& e)
        {
            LOG_WARNING(sLogger, ("ExceptionBase", e.ToString())
                ("FileName", fileName)("RetryCount", retryCount));
            if (++retryCount <= INT32_FLAG(append_store_LogFile_RetryCount))
            {
                usleep(sRandom.Get(INT32_FLAG(append_store_LogFile_MinSleepInterval),
                    INT32_FLAG(append_store_LogFile_MaxSleepInterval)));
            }
        }
    } while (retryCount <= INT32_FLAG(append_store_LogFile_RetryCount));

    LOG_ERROR(sLogger, ("OpenLog4Read", "Fail")("FileName", fileName));
    APSARA_THROW(AppendStoreExceptionBase, "OpenLog4Read fail, fileName:" + fileName);
}

bool PanguHelper::ReleaseLogFileLock(const std::string& fileName)
{
    int32_t retryCount = 0;
    do
    {
        try
        {
            FileSystem::GetInstance()->ReleaseLogFileLock(fileName);
            return true;
        }
        catch (const ExceptionBase& e)
        {
            LOG_WARNING(sLogger, ("ExceptionBase", e.ToString())
                ("FileName", fileName)("RetryCount", retryCount));
            if (++retryCount <= INT32_FLAG(append_store_LogFile_RetryCount))
            {
                usleep(sRandom.Get(INT32_FLAG(append_store_LogFile_MinSleepInterval),
                    INT32_FLAG(append_store_LogFile_MaxSleepInterval)));
            }
        }
    } while (retryCount <= INT32_FLAG(append_store_LogFile_RetryCount));

    LOG_ERROR(sLogger, ("ReleaseLogFileLock", "Fail")("FileName", fileName));
    return false;
}

