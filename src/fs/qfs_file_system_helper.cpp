#include <log4cxx/logger.h>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"

using namespace log4cxx;
//using namespace log4cxx::helpers;

// static logger variable
//LoggerPtr qfsfsh_logger(Logger::getLogger( "appendstore.qfsfsh_helper"));

QFSHelper::QFSHelper()
{
    kfsClient = NULL;
}

QFSHelper::~QFSHelper()
{
}

void QFSHelper::Connect() 
{	
    Connect(string("128.111.46.96"), 20000);
}

void QFSHelper::Connect(string metaserverhost, int metaserverport) 
{
    if (p_instance_ != NULL) {
        LOG4CXX_WARN(logger_, "File system helper is already initialized");
        return;
    }
    QFSHelper* p_qfs_helper = new QFSHelper();
    p_qfs_helper->kfsClient = KFS::Connect(metaserverhost, metaserverport);
    if (p_qfs_helper->kfsClient == NULL) {
		LOG4CXX_ERROR(logger_, "Failed to Connect to QFS Master Node ==> " << metaserverhost << ":" << metaserverport);
    }
    else
        LOG4CXX_INFO(logger_, "Connected to QFS Master Node");
    p_instance_ = dynamic_cast<FileSystemHelper*>(p_qfs_helper);
}

FileHelper* QFSHelper::CreateFileHelper(string fname, int mode)
{
    QFSHelper* p_qfsh = dynamic_cast<QFSHelper*>(FileSystemHelper::GetInstance());
    if (p_qfsh == NULL) {
        LOG4CXX_ERROR(logger_, "QFS file system helper is not initialized");
        return NULL;
    }
    FileHelper* p_fh = new QFSFileHelper(p_qfsh, fname, mode);
    return p_fh;
}

void QFSHelper::DestroyFileHelper(FileHelper* p_fh)
{
    delete p_fh;
}

bool QFSHelper::IsFileExists(string fname)
{
	bool value = kfsClient->Exists(fname.c_str());
	LOG4CXX_INFO(logger_, "IsFileExists(" << fname << ") - " << value);
    return value;
}

bool QFSHelper::IsDirectoryExists(string dirname)
{
	bool value = kfsClient->Exists(dirname.c_str());
	LOG4CXX_INFO(logger_, "IsDirectoryExists(" << dirname << ") - " << value);
    return value;
}

long QFSHelper::GetSize(string fname)
{
    KFS::KfsFileAttr kfsattr; 
    kfsClient->Stat(fname.c_str(), kfsattr); 
    LOG4CXX_INFO(logger_, "getSize(" << fname << ") - " << kfsattr.fileSize);
    return kfsattr.fileSize;
}

int QFSHelper::ListDir(string pathname, vector<string> &result)
{
    return kfsClient->Readdir(pathname.c_str(), result);
}

int QFSHelper::CreateDirectory(const string& pathname) {
    int res = kfsClient->Mkdirs(pathname.c_str());
    if (res < 0 && res != -EEXIST) {
		LOG4CXX_ERROR(logger_, "Directory Creation failed : " << pathname);
		THROW_EXCEPTION(FileCreationException, "Failure in directory Creation : " + pathname);
    }	
	LOG4CXX_INFO(logger_, "Directory Created(" << pathname << ")" );
    return res;
}

int QFSHelper::CreateFile(const string& pathname)
{
    int fd = kfsClient->Create(pathname.c_str());
    if (fd < 0) { 
		LOG4CXX_ERROR(logger_, "File Creation failed : " << pathname);
		THROW_EXCEPTION(FileCreationException, "Failed while creating file : " + pathname);
    }
    LOG4CXX_INFO(logger_, "File Created : " << pathname);
    return fd;
}

int QFSHelper::RemoveFile(const string& pathname)
{
    int res = kfsClient->Remove(pathname.c_str());
    if (res < 0) {
        LOG4CXX_ERROR(logger_, "file deletion failed : " << pathname);
        THROW_EXCEPTION(FileDeletionException, "Failed while deleting file : " + pathname);
    }
    LOG4CXX_INFO(logger_, "File deleted : " << pathname);
    return res;
}

int QFSHelper::RemoveDirectory(const string& dirname)
{
    int res = kfsClient->Rmdir(dirname.c_str());
    if (res < 0) {
        LOG4CXX_ERROR(logger_, "directory deletion failed : " << dirname);
        THROW_EXCEPTION(DirectoryDeletionException, "Failed while deleting directory : " + dirname);
    }
    LOG4CXX_INFO(logger_, "Directory deleted : " << dirname);
    return res;
}



















