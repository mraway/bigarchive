/*
 * this class performs all file related actions,
 * this class internally uses filesystem's client to get its work done
 */
#ifndef FILE_HELPER_H
#define FILE_HELPER_H

#include <log4cxx/logger.h>

extern "C" {
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
}

#include "file_system_helper.h"

using namespace log4cxx;
using namespace log4cxx;
using namespace log4cxx::helpers;

class FileHelper {
public:
    FileHelper() {}
    /* Creates a file */
    virtual void Create() {}
    /* Opens a file on specified mode */
    virtual void Open() {}
    /* Reads from file into buffer */
    virtual int Read(char *buffer, size_t length) {return -1;}
    /* Writes buffer into file */
    virtual int Write(char *buffer, size_t length) {return -1;}
    /* Write Data into file */
    virtual int WriteData(char *buffer, size_t length) {return -1;}
    /* Write and Sync */
    virtual int Flush(char *buffer, size_t length) {return -1;}
    /* Write Data and Sync it */
    virtual int FlushData(char *buffer, size_t length) {return -1;}
    /* Append */
    virtual int Append(char *buffer, int legnt) {return -1;}
    /* Seeks to position */
    virtual void Seek(uint64_t offset) {}
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
    /* logger for logging messages */
    static LoggerPtr logger_;
};

#endif /* FILE_HELPER_H */
