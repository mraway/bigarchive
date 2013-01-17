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
  FileHelper() {}
  /* Creates a file */
  virtual void Create() {}
  /* Opens a file on specified mode */
  virtual void Open() {}
  /* Reads from file into buffer */
  virtual int Read(char *buffer, int length) {return -1;}
  /* Writes buffer into file */
  virtual int Write(char *buffer, int length) {return -1;}
  /* Write Data into file */
  virtual int WriteData(char *buffer, int length) {return -1;}
  /* Write and Sync */
  virtual int Flush(char *buffer, int length) {return -1;}
  /* Write Data and Sync it */
  virtual int FlushData(char *buffer, int length) {return -1;}
  /* Seeks to position */
  virtual void Seek(int offset) {}
  /* */
  virtual uint32_t GetNextLogSize() {return 0;}
  /* Closes the file */
  virtual void Close() {}
 protected:
  /*file descriptor*/
  int fd;
  /* file modes - r, w, rw, a etc.*/
  int mode; 
  /* filename of the file that we are dealing with*/
  string filename;
};

#endif /* FILE_HELPER_H */
