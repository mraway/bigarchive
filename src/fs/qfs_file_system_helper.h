#ifndef QFS_FILESYSTEM_HELPER_H
#define QFS_FILESYSTEM_HELPER_H


#include "../include/file_system_helper.h"
#include "../include/KfsClient.h"
#include "../include/exception.h"
#include "../include/KfsAttr.h"
#include <iostream>
#include <fstream>
#include <cerrno>



extern "C" {
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

using std::string;

class QFSHelper : public FileSystemHelper {

public:
    /* override */ void Connect(); 
    /* override */ void Connect(string metaserverhost, int metaserverport);
    /* override */ void DisConnect();
    /* override */ bool IsFileExists(string fname);
    /* override */ bool IsDirectoryExists(string dirname);
    /* override */ long getSize(string fname);
    /* override */ int ListDir(string pathname, vector<string> &result);
    /* override */ int CreateDirectory(const string& pathname);
    /* override */ int CreateFile(const string& pathname);
    /* override */ int RemoveFile(const string& pathname);
    /* override */ int RemoveDirectory(const string& dirname);
public:	
    KFS::KfsClient *kfsClient;
};

#endif
