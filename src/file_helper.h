/*
 file_helper
*/

#ifndef FILE_HELPER_H
#define FILE_HELPER_H

class FileHelper {
 public:
  FileHelper();
  void Open();
  void Read();
  void Write();
  void Close();
 protected:
  int fd;
  int mode; 
  char *filename;
};

#endif /* FILE_HELPER_H */
