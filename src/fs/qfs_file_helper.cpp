#include "qfs_file_helper.h"

#include <iostream>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

//LoggerPtr logger_(Logger::getLogger( "appendstore.qfs_helper"));
//Karim: Each QFSFileHelper object has a QFSHelper object, a filename, mode, and a file descriptor associated with it.

/**
   Consturctor for File Helper, performs file related opereations
*/
QFSFileHelper::QFSFileHelper(QFSHelper *qfshelper, string fname, int mode) {
    //DOMConfigurator::configure("Log4cxxConfig.xml");
    this->qfshelper = qfshelper;
    this->filename = fname;
    this->mode = mode;
    this->fd = -1;
    LOG4CXX_DEBUG(logger_, "File helper created : " << fname );
}

/**
   Create file
*/
void QFSFileHelper::Create()
{
    fd = qfshelper->kfsClient->Create(filename.c_str());

    if (fd < 0) { 
        LOG4CXX_ERROR(logger_, "File Creation failed : " << filename);
        THROW_EXCEPTION(FileCreationException, "Failed while creating file : " + filename);
    }
    LOG4CXX_INFO(logger_, "File Created : " << filename);
}

/**
   Opens file on specified mode, due to the limitation of QFS,
   only two modes are expected: O_RDONLY, O_WRONLY,
   if application specify O_APPEND, we turn it into O_WRONLY, and seek the end
*/
void QFSFileHelper::Open() 
{
    if( ! qfshelper->IsFileExists(filename)) {
        Create();  // Karim: If file does not exist then create it.
    }

    bool append = false;

    if((mode & O_APPEND) != 0) {
        // for append write mode, open the file in write mode and seek to last.
        // because we found some issues with O_APPEND mode opening, 
        // QFS append mode doesn't report correct file size
        append = true;
        mode = O_WRONLY;
    }

    fd = qfshelper->kfsClient->Open(filename.c_str(), mode);

    if(fd < 0) { 
        LOG4CXX_ERROR(logger_, "Failed while opening file : " << filename << ", ERROR :" << KFS::ErrorCodeToStr(fd));
        THROW_EXCEPTION(FileOpenException, "Failed while opening file : " + filename + " ERROR : " + KFS::ErrorCodeToStr(fd));
    }

    /* Seeking to the last */
    if(append) {
        LOG4CXX_DEBUG(logger_, "open under append mode: " << filename);
        Seek(qfshelper->GetSize(filename));
    }

    LOG4CXX_DEBUG(logger_, "File Opened: " << filename << 
                 ", mode is " << get_mode() << 
                 ", position at " << qfshelper->kfsClient->Tell(fd));
}

void QFSFileHelper::Close() 
{
    if(fd < 0) {
        LOG4CXX_WARN(logger_, "file is not opened: " << filename); 
    }
    else {
        if((mode & O_WRONLY) != 0) {
            qfshelper->kfsClient->Sync(fd);
        }
        qfshelper->kfsClient->Close(fd);
        LOG4CXX_INFO(logger_, "File Synced and Closed: " << filename); 
    }
}


int QFSFileHelper::Read(char *buffer, size_t length) {
    /* check whether its opened of not */
    if (fd == -1) {
        Open();
    }
    LOG4CXX_DEBUG(logger_, "Trying to read " << length << " bytes from file(" << filename << ") at " << qfshelper->kfsClient->Tell(fd));

    //Karim: If file already open then read "length" bytes and put them in buffer
    size_t bytes_read = qfshelper->kfsClient->Read(fd, buffer, length);

    if (bytes_read != length) {
        if (bytes_read < 0) {
            LOG4CXX_ERROR(logger_, "Failed while reading from file(" << filename << ") - ERROR : " << KFS::ErrorCodeToStr(bytes_read));
            THROW_EXCEPTION(AppendStoreReadException, "Failed while reading file(" + filename + ") - ERROR : " + KFS::ErrorCodeToStr(bytes_read));
        }
        else {
            LOG4CXX_ERROR(logger_, "Less number of bytes read from file than specified");
        }
    }

    return bytes_read;	
}

/**
 * Write and Flush - returns the current write position (got by calling Tell)
 * WriteData and FlushData - returns the number of bytes wrote
 */ 

int QFSFileHelper::Write(char *buffer, size_t length) {
    // why care !! 
    if(fd == -1) {	
        LOG4CXX_ERROR(logger_, "file not opened :" << filename);
    }

    Header header(length);
    int dataLength = length + sizeof(Header);    
    string data(dataLength, 0);

    memcpy(&data[0], &header, sizeof(Header));
    memcpy(&data[sizeof(Header)], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->Write(fd, data.c_str(), dataLength);

    if( bytes_wrote != dataLength) {
        string bytes_wrote_str = "" + bytes_wrote;
        string length_str = "" + dataLength;
        LOG4CXX_ERROR(logger_, "Was able to write only " << bytes_wrote << " bytes , instead of " << length);
        THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
    }

    LOG4CXX_DEBUG(logger_, "Wrote " << length << " bytes into file(" << filename << ")");    
    return qfshelper->kfsClient->Tell(fd);
    // return x;
}

int QFSFileHelper::Append(char *buffer, size_t length) {
    int dataLength = length + sizeof(Header);
    Header header(length);

    string data(dataLength, 0);
    memcpy(&data[0], &header, sizeof(Header));
    memcpy(&data[sizeof(Header)], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->AtomicRecordAppend(fd, data.c_str(), dataLength);


    if( bytes_wrote != dataLength) {
        string bytes_wrote_str = "" + bytes_wrote;
        string length_str = "" + dataLength;
        LOG4CXX_ERROR(logger_, "Was able to append only " << bytes_wrote_str << " bytes , instead of " << length_str);
        LOG4CXX_ERROR(logger_, "Error " << KFS::ErrorCodeToStr(bytes_wrote));  
        THROW_EXCEPTION(AppendStoreWriteException,  "Was able to append only " + bytes_wrote_str + ", instead of " + length_str);
    }

    LOG4CXX_DEBUG(logger_, "Append " << length << " bytes into file(" << filename << ")");    

    return bytes_wrote;
}

int QFSFileHelper::WriteData(char *buffer, size_t length) {

    if(fd == -1) {
        LOG4CXX_ERROR(logger_, "file not opened :" << filename);
    }

    size_t bytes_wrote = qfshelper->kfsClient->Write(fd, buffer, length);
    if( bytes_wrote != length) {
        string bytes_wrote_str = "" + bytes_wrote;
        string length_str = "" + length;
        LOG4CXX_ERROR(logger_, "Was able to write only " << bytes_wrote_str << ", instead of " << length_str);
        THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
    }

    LOG4CXX_DEBUG(logger_, "WriteDATA " << length << " bytes into file(" << filename << ")");    

    return bytes_wrote;
}

int QFSFileHelper::Flush(char *buffer, size_t length) {
    int pos = Write(buffer, length);
    qfshelper->kfsClient->Sync(fd);
    LOG4CXX_DEBUG(logger_, " calling tell instead of size in Flush : " << pos);
    return pos; 
}


int QFSFileHelper::FlushData(char *buffer, size_t length) { 
    int bytes_wrote = WriteData(buffer, length);
    qfshelper->kfsClient->Sync(fd);
    return bytes_wrote;
}


void QFSFileHelper::Seek(uint64_t offset) {
    LOG4CXX_DEBUG(logger_, "seek to " << offset);
    qfshelper->kfsClient->Seek(fd, offset);
}


//Karim: I think this function reads the header part from a file and return the real data length
uint32_t QFSFileHelper::GetNextLogSize() {
    char *buffer = new char[sizeof(Header)];
    /* setting the buffer to zero, to avoid garbage data */
    memset(buffer, 0, sizeof(Header));
    Header *header = new Header(-1);// Karim: I think the new here is unnecessary because the pointer will later be reassigned??
    Read(buffer, sizeof(Header));
    header = (Header *)buffer;
    LOG4CXX_DEBUG(logger_, "GetNextLogSize - " << filename << " - " << header->data_length);    
    return header->data_length;
}


string QFSFileHelper::get_mode() {
    switch(mode) {
    case O_RDONLY : return "READ_ONLY";
    case O_WRONLY : return "WRITE_ONLY";
    case O_APPEND : return "APPEND";
    default : return "DEFAULT";
    }
}
