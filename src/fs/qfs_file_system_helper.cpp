#include "qfs_file_system_helper.h"
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// static logger variable
LoggerPtr qfsfsh_logger(Logger::getLogger( "appendstore.qfsfsh_helper"));


void QFSHelper::Connect() 
{	
    DOMConfigurator::configure("Log4cxxConfig.xml");
    Connect(string("128.111.46.96"), 20000);
    LOG4CXX_INFO(qfsfsh_logger, "Connected to QFS Master Node");
}

void QFSHelper::Connect(string metaserverhost, int metaserverport) 
{
    kfsClient = KFS::Connect(metaserverhost, metaserverport);
    if (! kfsClient ) {
		LOG4CXX_ERROR(qfsfsh_logger, "Failed to Connect to QFS Master Node ==> " << metaserverhost << ":" << metaserverport);
    }
}

void QFSHelper::DisConnect()
{
	kfsClient = NULL;
}

bool QFSHelper::IsFileExists(string fname)
{
	bool value = kfsClient->Exists(fname.c_str());
	LOG4CXX_INFO(qfsfsh_logger, "IsFileExists(" << fname << ") - " << value);
    return value;
}

bool QFSHelper::IsDirectoryExists(string dirname)
{
	bool value = kfsClient->Exists(dirname.c_str());
	LOG4CXX_INFO(qfsfsh_logger, "IsDirectoryExists(" << dirname << ") - " << value);
    return value;
}

long QFSHelper::getSize(string fname)
{
    KFS::KfsFileAttr kfsattr; 
    kfsClient->Stat(fname.c_str(), kfsattr); 
    LOG4CXX_INFO(qfsfsh_logger, "getSize(" << fname << ") - " << kfsattr.fileSize);
    return kfsattr.fileSize;
}

int QFSHelper::ListDir(string pathname, vector<string> &result)
{
    return kfsClient->Readdir(pathname.c_str(), result);
}

int QFSHelper::CreateDirectory(const string& pathname) {
    int res = kfsClient->Mkdirs(pathname.c_str());
    if (res < 0 && res != -EEXIST) {
		LOG4CXX_ERROR(qfsfsh_logger, "Directory Creation failed : " << pathname);
		THROW_EXCEPTION(FileCreationException, "Failure in directory Creation : " + pathname);
    }	
	LOG4CXX_INFO(qfsfsh_logger, "Directory Created(" << pathname << ")" );
    return res;
}

int QFSHelper::CreateFile(const string& pathname)
{
    int fd = kfsClient->Create(pathname.c_str());
    if (fd < 0) { 
		LOG4CXX_ERROR(qfsfsh_logger, "File Creation failed : " << pathname);
		THROW_EXCEPTION(FileCreationException, "Failed while creating file : " + pathname);
    }
    LOG4CXX_INFO(qfsfsh_logger, "File Created : " << pathname);
    return fd;
}

int QFSHelper::RemoveFile(const string& pathname)
{
    int res = kfsClient->Remove(pathname.c_str());
    if (res < 0) {
        LOG4CXX_ERROR(qfsfsh_logger, "file deletion failed : " << pathname);
        THROW_EXCEPTION(FileDeletionException, "Failed while deleting file : " + pathname);
    }
    LOG4CXX_INFO(qfsfsh_logger, "File deleted : " << pathname);
    return res;
}

int QFSHelper::RemoveDirectory(const string& dirname)
{
    int res = kfsClient->Rmdir(dirname.c_str());
    if (res < 0) {
        LOG4CXX_ERROR(qfsfsh_logger, "directory deletion failed : " << dirname);
        THROW_EXCEPTION(DirectoryDeletionException, "Failed while deleting directory : " + dirname);
    }
    LOG4CXX_INFO(qfsfsh_logger, "Directory deleted : " << dirname);
    return res;
}



















