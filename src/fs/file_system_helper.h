/*
File System helper header
*/

#ifndef FILE_SYSTEM_HELPER_H
#define FILE_SYSTEM_HELPER_H

class FileSystemHelper {
 public:
  /* Connect method establishes a connection with FileSystem MetaServer (usually host name and port are passed)*/
  void Connect() {}
  /* list file contents */
  int Readdir(const char *pathname, vector<string> &result) {}
  /* checks whether file "fname" exists or not */
  bool IsFileExists(char *fname) {}
  /* checks whether directory "dirname" exits or not */
  bool IsDirectoryExists(char *dirname) {}
  /* gets file Size */
  int getSize(char *fname) {}
};

#endif /* FILE_SYSTEM_HELPER_H */
