#include "qfs_file_helper.h"

#include <iostream>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

LoggerPtr qfsfh_logger(Logger::getLogger( "appendstore.qfs_helper"));


/**
Consturctor for File Helper, performs file related opereations
*/
QFSFileHelper::QFSFileHelper(QFSHelper *qfshelper, string fname, int mode) {
    DOMConfigurator::configure("/home/prakash/log_config.xml");
    this->qfshelper = qfshelper;
    this->filename = fname;
    this->mode = mode;
    this->fd = -1;
    LOG4CXX_INFO(qfsfh_logger, "File helper created : " << fname );
}

/**
Create file
*/
void QFSFileHelper::Create()
{
    fd = qfshelper->kfsClient->Create(filename.c_str());

    if (fd < 0) { 
		LOG4CXX_ERROR(qfsfh_logger, "File Creation failed : " << filename);
		THROW_EXCEPTION(FileCreationException, "Failed while creating file : " + filename);
    }
    LOG4CXX_INFO(qfsfh_logger, "File Created : " << filename);
}

/**
Opens file on specified mode
*/
void QFSFileHelper::Open() {

    if( ! qfshelper->IsFileExists(filename)) {
		Create();
	}

    bool append = false;

    if(mode == O_APPEND) {
		/* for append mode open the file in Write mode and seek to last */
		/* found some issue with O_APPEND mode opening ! */
		append = true;
		mode = O_WRONLY;
    }

    fd = qfshelper->kfsClient->Open(filename.c_str(), mode);

    if(fd < 0) { 
		LOG4CXX_ERROR(qfsfh_logger, "Failed while opening file : " << filename << ", ERROR :" << KFS::ErrorCodeToStr(fd));
		THROW_EXCEPTION(FileOpenException, "Failed while opening file : " + filename + " ERROR : " + KFS::ErrorCodeToStr(fd));
    }

	/* Seeking to the last */
    if(append) {
		mode = O_APPEND;
		LOG4CXX_INFO(qfsfh_logger, "opening in Write mode and seeking to end of file : " << filename);
		Seek(qfshelper->getSize(filename));
    }
	LOG4CXX_INFO(qfsfh_logger, "File Opened : filename(" << filename << "), mode(" << get_mode() << ")");
}

void QFSFileHelper::Close() {
   

    if(fd < 0) {
     LOG4CXX_INFO(qfsfh_logger, "FileHelper:Close : fd < 0 : " << filename); 
    }
    else {
     qfshelper->kfsClient->Sync(fd);
     qfshelper->kfsClient->Close(fd);
     LOG4CXX_INFO(qfsfh_logger, "file closed : " << filename); 
    }
}

int QFSFileHelper::Read(char *buffer, int length) {
    /* check whether its opened of not */
    if(fd == -1)
	Open();

    LOG4CXX_INFO(qfsfh_logger, " Reading from file " << filename << ", length " << length);    

    int bytes_read = qfshelper->kfsClient->Read(fd, buffer, length);

    if(bytes_read != length) {
	if(bytes_read < 0) {
	    LOG4CXX_ERROR(qfsfh_logger, "Failure while reading file " << filename << " ERROR : " << KFS::ErrorCodeToStr(bytes_read));
	    THROW_EXCEPTION(AppendStoreReadException, "Failed while reading file " + filename + " ERROR : " + KFS::ErrorCodeToStr(bytes_read));
	}
	else {
	    LOG4CXX_ERROR(qfsfh_logger, "Less number of bytes read from file than specified");
	}
    }
    return bytes_read;	
}

int QFSFileHelper::Write(char *buffer, int length) {
    /* check whether its opened or not */
    if(fd == -1)
      Open();

    int dataLength = length + sizeof(Header);
    Header header(length);

    string data(dataLength, 0);
    memcpy(&data[0], &header, sizeof(Header));
    memcpy(&data[sizeof(Header)], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->Write(fd, data.c_str(), dataLength);

    if( bytes_wrote != dataLength) {
	string bytes_wrote_str = "" + bytes_wrote;
	string length_str = "" + dataLength;
	LOG4CXX_ERROR(qfsfh_logger, "Was able to write only " << bytes_wrote_str << " bytes , instead of " << length_str);
	THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
    }
    
    return bytes_wrote;
}

int QFSFileHelper::Append(char *buffer, int length) {

 if(fd == -1)
      Open();

    int dataLength = length + sizeof(Header);
    Header header(length);

    string data(dataLength, 0);
    memcpy(&data[0], &header, sizeof(Header));
    memcpy(&data[sizeof(Header)], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->AtomicRecordAppend(fd, data.c_str(), dataLength);


    if( bytes_wrote != dataLength) {
        string bytes_wrote_str = "" + bytes_wrote;
        string length_str = "" + dataLength;
        LOG4CXX_ERROR(qfsfh_logger, "XXXXXXXXXXXX Was able to append only " << bytes_wrote_str << " bytes , instead of " << length_str);
        LOG4CXX_ERROR(qfsfh_logger, "Error " << KFS::ErrorCodeToStr(bytes_wrote));  
        // THROW_EXCEPTION(AppendStoreWriteException,  "Was able to append only " + bytes_wrote_str + ", instead of " + length_str);
    }


    return bytes_wrote;
}

int QFSFileHelper::WriteData(char *buffer, int length) {
   if(fd == -1)
      Open();

    int dataLength = length;

    string data(dataLength, 0);
    memcpy(&data[0], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->Write(fd, data.c_str(), dataLength);

    
    if( bytes_wrote != dataLength) {
                string bytes_wrote_str = "" + bytes_wrote;
                string length_str = "" + dataLength;
                LOG4CXX_ERROR(qfsfh_logger, "Was able to write only " << bytes_wrote_str << ", instead of " << length_str);
                THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
    }
    
    return bytes_wrote;
}

int QFSFileHelper::Flush(char *buffer, int length) {
    int bytes_wrote = Write(buffer, length);
    qfshelper->kfsClient->Sync(fd);
    return qfshelper->getSize(filename);
}


int QFSFileHelper::FlushData(char *buffer, int length) { 
    int bytes_wrote = WriteData(buffer, length);
    qfshelper->kfsClient->Sync(fd);
    return qfshelper->getSize(filename);
}


void QFSFileHelper::Seek(int offset) {
    qfshelper->kfsClient->Seek(fd, offset);
}

uint32_t QFSFileHelper::GetNextLogSize() {
    char *buffer = new char[sizeof(Header)];
    //for(int i=0; i < sizeof(Header); i++) {
    // *(buffer + i) = 0;
    //}
    memset(buffer, 0, sizeof(Header));
    Header *header = new Header(-1);
    Read(buffer, sizeof(Header));
    header = (Header *)buffer;
    return header->data_length;
}


string get_mode() {
	switch(mode) {
		case O_RDONLY : return "READ_ONLY";
		case O_WRONLY : return "WRITE_ONLY";
		case O_APPEND : return "APPEND";
		default : return "DEFAULT";
	}
}



