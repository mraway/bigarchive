#include "pangu_helper.h"
#include "append_store_exception.h"
#include "append_store.h"


using namespace apsara::pangu;
using namespace apsara::AppendStore;


Store* StoreFactory::Create(const StoreParameter& para, const std::string& type)
{
    if (!type.compare(panguStore))
    {
        if (para.mAppend != true)
        {
            APSARA_THROW(AppendStoreFactoryException, "Has to be appendable for StoreFactory::Create");
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
            APSARA_THROW(AppendStoreFactoryException, "Has to be non-appendable for StoreFactory::Load");
        }
        return new PanguAppendStore(para, false);
    }
    return NULL;
}


uint64_t StoreUtility::GetSize(const std::string& rootPath) 
{
    std::string path = rootPath;
    if (path.compare(path.size()-1, 1, "/"))
    {
        path.append("/");
    }
    
    // if does not exist at the top level, not throw, just return 0
    if (PanguHelper::IsDirectoryExist(path))
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
    try
    {
        pangu::FileSystem::GetInstance()->ListDirectory(dirName, "", AppendStore::DF_MAXFILENO, data_files);
    }
    catch (apsara::DirectoryNotExistException& e)
    {
        return 0;
    }
    catch (apsara::ExceptionBase& e)
    {
        APSARA_THROW(AppendStoreReadException, "GetDirectorySize " + e.ToString());
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
                total_size += PanguHelper::GetFileSize(dirName+data_files.at(i));
            }
        }
        return total_size;
    }
    catch (apsara::ExceptionBase& e)
    {
        APSARA_THROW(AppendStoreReadException, "GetDirectorySize " + e.ToString());
    }
}

