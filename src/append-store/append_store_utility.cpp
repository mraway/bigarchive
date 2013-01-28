// #include "pangu_helper.h"
#include "../exception/exception.h"
#include "append_store.h"


Store* StoreFactory::Create(const StoreParameter& para, const std::string& type)
{
    if (!type.compare(panguStore))
    {
        if (para.mAppend != true)
        {
            THROW_EXCEPTION(AppendStoreFactoryException, "Has to be appendable for StoreFactory::Create");
        }
        return new PanguAppendStore(para, true);
    }
    return NULL;
}
    
Store* StoreFactory::Load(const StoreParameter& para, const std::string& type) 
{
    if (!type.compare(panguStore))
    {
        if (para.mAppend != false)
        {
            THROW_EXCEPTION(AppendStoreFactoryException, "Has to be non-appendable for StoreFactory::Load");
        }
        return new PanguAppendStore(para, false);
    }
    return NULL;
}

/*
uint64_t StoreUtility::GetSize(const std::string& rootPath) 
{
    std::string path = rootPath;
    if (path.compare(path.size()-1, 1, "/"))
    {
        path.append("/");
    }
    
    // if does not exist at the top level, not throw, just return 0
    // CHKIT FileSystemHelper->IsDirectoryExist(path) 
    // do we need to declare a new FileSystemHelper ??
    if (IsDirectoryExist(path))
    {
        return GetDirectorySize(path);
    }
    return 0;
}

uint64_t StoreUtility::GetDirectorySize(const std::string& dirName)
{
    // FileNotExistException and DirectoryNotExistException handled 

    uint64_t total_size = 0;
    std::vector<std::string> data_files;

    QFSHelper *qfsHelper = new QFSHelper();
    qfsHelper->Connect("host", 30000);
    try
    {
        // pangu::FileSystem::GetInstance()->ListDirectory(dirName, "", DF_MAXFILENO, data_files);
	qfsHelper->ListDir(dirName.c_str(), data_files);
    }
    catch (DirectoryNotExistException& e)
    {
        return 0;
    }
    catch (ExceptionBase& e)
    {
        THROW_EXCEPTION(AppendStoreReadException, "GetDirectorySize " + e.ToString());
    }

    try
    {
        for (uint32_t i=0; i< data_files.size(); i++)
        {
            if (data_files.at(i).rfind('/') == data_files.at(i).size()-1)
            {
                total_size += GetDirectorySize(dirName+data_files.at(i));
            }
            else
            {
                total_size += GetFileSize(dirName+data_files.at(i));
            }
        }
        return total_size;
    }
    catch (ExceptionBase& e)
    {
        THROW_EXCEPTION(AppendStoreReadException, "GetDirectorySize " + e.ToString());
    }
}
*/
