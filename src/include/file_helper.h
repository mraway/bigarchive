/*
 file_helper
*/


#ifndef FILE_HELPER_H
#define FILE_HELPER_H

#include "file_system_helper.h"
#include <stdint.h>
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
  int Read(char *buffer, int length) {}
  /* Writes buffer into file */
  int Write(char *buffer, int length) {}
  /* Write and Sync */
  int Flush(char *buffer, int length) {}
  /* Seeks to position */
  void Seek(int offset) {}
  /* */
  uint32_t GetNextLogSize() {return 0;}
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
