/*
  File System helper header
*/

#ifndef FILE_SYSTEM_HELPER_H
#define FILE_SYSTEM_HELPER_H

#include<vector>
#include<string>

using std::vector;
using std::string;

class FileSystemHelper {
public:
    /* Connect method establishes a connection with FileSystem MetaServer (usually host name and port are passed)*/
    virtual void Connect() = 0;
    virtual void Connect(string host, int port) = 0;
    /* */
    virtual void DisConnect() = 0;
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
};

#endif /* FILE_SYSTEM_HELPER_H */
