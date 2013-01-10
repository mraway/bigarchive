#include "qfs_file_system_helper.h"


#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// static logger variable
LoggerPtr qfsfsh_logger(Logger::getLogger( "appendstore.qfs_helper"));


void QFSHelper::Connect() 
{	
    Connect(string("localhost"), 20000);
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
	LOG4CXX_ERROR(qfsfsh_logger, "Directory Creation failed : " << pathname);
	THROW_EXCEPTION(FileCreationException, "Failure in directory Creation : " + pathname);
    }	
    return ret;
}


