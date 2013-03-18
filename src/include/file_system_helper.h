/*
  File System helper header
*/

#ifndef FILE_SYSTEM_HELPER_H
#define FILE_SYSTEM_HELPER_H

#include <log4cxx/logger.h>

extern "C" {
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

using namespace std;
using namespace log4cxx;
using namespace log4cxx::helpers;

class FileHelper;

class FileSystemHelper {
public:
    /* return the global single instance of file system helper */
    static FileSystemHelper* GetInstance();
    /* create and return a file helper object to manipulate files */
    virtual FileHelper* CreateFileHelper(string fname, int mode) = 0;
    /* destroy a file helper */
    virtual void DestroyFileHelper(FileHelper* p_fh) = 0;
    /* list file contents */
    virtual int ListDir(string pathname, vector<string> &result) = 0;
    /* checks whether file "fname" exists or not */
    virtual bool IsFileExists(string fname) = 0;
    /* checks whether directory "dirname" exits or not */
    virtual bool IsDirectoryExists(string dirname) = 0;
    /* gets file Size */
    virtual long GetSize(string fname) = 0;
    /* create Directory */
    virtual int CreateDirectory(const string& dirname) = 0;
    /* create a file */
    virtual int CreateFile(const string& pathname) = 0;
    /* remove a file */
    virtual int RemoveFile(const string& pathname) = 0;
    /* remove a dir */
    virtual int RemoveDirectory(const string& dirname) = 0;
protected:
    FileSystemHelper() {};
    virtual ~FileSystemHelper() {};
protected:
    static FileSystemHelper* p_instance_;
    static LoggerPtr logger_;
};

#endif /* FILE_SYSTEM_HELPER_H */
