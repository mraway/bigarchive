/*
 file_helper
*/

#include "file_system_helper.h"

#ifndef FILE_HELPER_H
#define FILE_HELPER_H

/*
 * this class performs all file related actions,
 * this class internally uses filesystem's client to get its word done
 */
class FileHelper {
 public:
  FileHelper();
  /* Creates a file */
  void Create() {}
  /* Opens a file on specified mode */
  void Open() {}
  /* Reads from file into buffer */
  void Read(char *buffer, int length) {}
  /* Writes buffer into file */
  void Write(char *buffer, int length) {}
  /* Seeks to position */
  void Seek(int offset) {}
  /* Closes the file */
  void Close() {}
 protected:
  /*file descriptor*/
  int fd;
  /* file modes - r, w, rw, a etc.*/
  int mode; 
  /* filename of the file that we are dealing with*/
  string filename;
};

#endif /* FILE_HELPER_H */
