#include "../include/file_system_helper.h"

FileSystemHelper* FileSystemHelper::p_instance_ = NULL;

LoggerPtr FileSystemHelper::logger_ = Logger::getLogger("FileSystemHelper");

FileSystemHelper* FileSystemHelper::GetInstance()
{
    if (p_instance_ == NULL)
        LOG4CXX_ERROR(logger_, "File system helper is not initialized");
    return p_instance_;
}
