/*
 file_helper
*/

#include<file_system_helper.h>

#ifndef FILE_HELPER_H
#define FILE_HELPER_H

class FileHelper {
 public:
  FileHelper();
  void Open();
  void Read(char *buffer, int length);
  void Write(char *buffer, int length);
  void Close();
 protected:
  int fd;
  int mode; 
  string filename;
};

#endif /* FILE_HELPER_H */
