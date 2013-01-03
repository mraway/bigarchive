#include "qfs_file_system_helper.h"


void QFSHelper::Connect() 
{	
    Connect(string("localhost"), 40000);
    LOG4CXX_INFO(logger, "Connected to QFS Master Node");
}

void QFSHelper::Connect(string metaserverhost, int metaserverport) 
{
    kfsClient = KFS::Connect(metaserverhost, metaserverport);
    if (! kfsClient ) {
	LOG4CXX_ERROR(logger, "Failed to Connect to QFS Master Node ==> " << metaserverhost << ":" << metaserverport);
    }
}

bool QFSHelper::IsFileExists(string fname)
{
    return kfsClient->Exists(fname.c_str());
}

bool QFSHelper::IsDirectoryExists(string dirname)
{
    return kfsClient->Exists(dirname.c_str());
}

int QFSHelper::getSize(string fname)
{
    KFS::KfsFileAttr kfsattr; // = new KfsFileAttr(); // Stat method takes reference of this object
    kfsClient->Stat(fname.c_str(), kfsattr, true); // true is for computing the size
    return kfsattr.fileSize;
}

int QFSHelper::ListDir(string pathname, vector<string> &result)
{
    return kfsClient->Readdir(pathname.c_str(), result);
}

int QFSHelper::CreateDirectory(string pathname) {
    int ret = kfsClient->Mkdir(pathname.c_str()); // mode is 0777 by default
    if(ret != 0) {
	LOG4CXX_ERROR(logger, "Directory Creation failed : " << pathname);
	THROW_EXCEPTION(FileCreationException, "Failure in directory Creation : " + pathname);
    }	
    return ret;
}


