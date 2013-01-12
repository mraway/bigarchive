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
  virtual void Connect() {}
  virtual void Connect(string host, int port) {}
  /* */
  virtual void DisConnect() {}
  /* list file contents */
  virtual int ListDir(string pathname, vector<string> &result) {return -1;}
  /* checks whether file "fname" exists or not */
  virtual bool IsFileExists(string fname) {return false;}
  /* checks whether directory "dirname" exits or not */
  virtual bool IsDirectoryExists(string dirname) {return false;}
  /* gets file Size */
  virtual int getSize(string fname) {return 0;}
  /* create Directory */
  virtual int CreateDirectory(string dirname) {return 0;}
};

#endif /* FILE_SYSTEM_HELPER_H */
