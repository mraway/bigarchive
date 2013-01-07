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
  void Connect() {}
  void Connect(string host, int port) {}
  /* list file contents */
  int ListDir(string pathname, vector<string> &result) {return -1;}
  /* checks whether file "fname" exists or not */
  bool IsFileExists(string fname) {return false;}
  /* checks whether directory "dirname" exits or not */
  bool IsDirectoryExists(string dirname) {return false;}
  /* gets file Size */
  int getSize(string fname) {return -1;}
  /* create Directory */
  int CreateDirectory(string dirname) {return -1;}
};

#endif /* FILE_SYSTEM_HELPER_H */
