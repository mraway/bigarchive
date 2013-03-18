#ifndef QFS_FILESYSTEM_HELPER_H
#define QFS_FILESYSTEM_HELPER_H

#include "../include/file_system_helper.h"
#include "../include/file_helper.h"
#include "../include/KfsClient.h"
#include "../include/exception.h"
#include "../include/KfsAttr.h"
#include <iostream>
#include <fstream>
#include <cerrno>

using std::string;

class QFSFileHelper;

class QFSHelper : public FileSystemHelper {

public:
    /*
     *  Create the single instance of file system helper,
     *  shall be called at the beginning of main program to initiate a connection to QFS.
     */
    static void Connect(); 
    static void Connect(string metaserverhost, int metaserverport);
    /* override */ FileHelper* CreateFileHelper(string fname, int mode);
    /* override */ void DestroyFileHelper(FileHelper* p_fh);
    /* override */ bool IsFileExists(string fname);
    /* override */ bool IsDirectoryExists(string dirname);
    /* override */ long GetSize(string fname);
    /* override */ int ListDir(string pathname, vector<string> &result);
    /* override */ int CreateDirectory(const string& pathname);
    /* override */ int CreateFile(const string& pathname);
    /* override */ int RemoveFile(const string& pathname);
    /* override */ int RemoveDirectory(const string& dirname);
public:	
    KFS::KfsClient *kfsClient;
protected:
    QFSHelper();
    /* suppose to disconnect QFS here, but we didn't find a disconnect API in QFS */
    ~QFSHelper();
};

#endif
