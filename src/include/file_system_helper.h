/*
File System helper header
*/

#include<vector>
#include<string>

#ifndef FILE_SYSTEM_HELPER_H
#define FILE_SYSTEM_HELPER_H

using std::vector;
using std::string;

class FileSystemHelper {
 public:
  /* Connect method establishes a connection with FileSystem MetaServer (usually host name and port are passed)*/
  void Connect() {}
  /* list file contents */
  int ListDir(const char *pathname, vector<string> &result) {}
  /* checks whether file "fname" exists or not */
  bool IsFileExists(char *fname) {}
  /* checks whether directory "dirname" exits or not */
  bool IsDirectoryExists(char *dirname) {}
  /* gets file Size */
  int getSize(char *fname) {}
};

#endif /* FILE_SYSTEM_HELPER_H */
