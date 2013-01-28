#ifndef QFS_FILESYSTEM_HELPER_H
#define QFS_FILESYSTEM_HELPER_H


#include "file_system_helper.h"
#include "KfsClient.h"
#include "../exception/exception.h"
#include "KfsAttr.h"
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
    void Connect(); 
    void Connect(string metaserverhost, int metaserverport);
    void DisConnect();
    bool IsFileExists(string fname);
    bool IsDirectoryExists(string dirname);
    long getSize(string fname);
    int ListDir(string pathname, vector<string> &result);
    int CreateDirectory(string pathname);

public:	
    KFS::KfsClient *kfsClient;
};

#endif
