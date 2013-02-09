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
    DOMConfigurator::configure("Log4cxxConfig.xml");
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
		LOG4CXX_INFO(qfsfh_logger, "Opening in Write mode and seeking to end of file : " << filename);
		Seek(qfshelper->getSize(filename));
    }

	LOG4CXX_INFO(qfsfh_logger, "File Opened : filename(" << filename << "), mode(" << get_mode() << ")");
}

void QFSFileHelper::Close() {

    if(fd < 0) {
    	LOG4CXX_INFO(qfsfh_logger, "File Close : file descriptor, fd < 0 for file : " << filename); 
    }
    else {
    	qfshelper->kfsClient->Sync(fd);
    	qfshelper->kfsClient->Close(fd);
    	LOG4CXX_INFO(qfsfh_logger, "File Synced and Closed : " << filename); 
    }
}

int QFSFileHelper::Read(char *buffer, int length) {
    /* check whether its opened of not */
    if(fd == -1) {
		Open();
	}

    int bytes_read = qfshelper->kfsClient->Read(fd, buffer, length);

    if(bytes_read != length) {
		if(bytes_read < 0) {
		    LOG4CXX_ERROR(qfsfh_logger, "Failed while reading from file(" << filename << ") - ERROR : " << KFS::ErrorCodeToStr(bytes_read));
		    THROW_EXCEPTION(AppendStoreReadException, "Failed while reading file(" + filename + ") - ERROR : " + KFS::ErrorCodeToStr(bytes_read));
		}
		else {
		    LOG4CXX_ERROR(qfsfh_logger, "Less number of bytes read from file than specified");
		}
    }

	LOG4CXX_INFO(qfsfh_logger, "Read " << length << " bytes from file(" << filename << ")");    

    return bytes_read;	
}

int QFSFileHelper::Write(char *buffer, int length) {

    if(fd == -1) {
    	Open();
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
		LOG4CXX_ERROR(qfsfh_logger, "Was able to write only " << bytes_wrote << " bytes , instead of " << length);
		THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
    }

    LOG4CXX_INFO(qfsfh_logger, "Wrote " << length << " bytes into file(" << filename << ")");    
    return qfshelper->kfsClient->Tell(fd);
    // return x;
}

int QFSFileHelper::Append(char *buffer, int length) {

	if(fd == -1) {
    	Open();
	}

    int dataLength = length + sizeof(Header);
    Header header(length);

    string data(dataLength, 0);
    memcpy(&data[0], &header, sizeof(Header));
    memcpy(&data[sizeof(Header)], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->AtomicRecordAppend(fd, data.c_str(), dataLength);


    if( bytes_wrote != dataLength) {
        string bytes_wrote_str = "" + bytes_wrote;
        string length_str = "" + dataLength;
        LOG4CXX_ERROR(qfsfh_logger, "Was able to append only " << bytes_wrote_str << " bytes , instead of " << length_str);
        LOG4CXX_ERROR(qfsfh_logger, "Error " << KFS::ErrorCodeToStr(bytes_wrote));  
        THROW_EXCEPTION(AppendStoreWriteException,  "Was able to append only " + bytes_wrote_str + ", instead of " + length_str);
    }

	LOG4CXX_INFO(qfsfh_logger, "Append " << length << " bytes into file(" << filename << ")");    

    return bytes_wrote;
}

int QFSFileHelper::WriteData(char *buffer, int dataLength) {
    if(fd == -1) {
	   	Open();
    }

/*
    int dataLength = length;
    string data(dataLength, 0);
    memcpy(&data[0], buffer, length);
*/
    int bytes_wrote = qfshelper->kfsClient->Write(fd, buffer, dataLength);

    
    if( bytes_wrote != dataLength) {
                string bytes_wrote_str = "" + bytes_wrote;
                string length_str = "" + dataLength;
                LOG4CXX_ERROR(qfsfh_logger, "Was able to write only " << bytes_wrote_str << ", instead of " << length_str);
                THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
    }

	LOG4CXX_INFO(qfsfh_logger, "WriteDATA " << dataLength << " bytes into file(" << filename << ")");    
    
    return bytes_wrote;
}

int QFSFileHelper::Flush(char *buffer, int length) {
    int pos = Write(buffer, length);
    qfshelper->kfsClient->Sync(fd);
    LOG4CXX_INFO(qfsfh_logger, " calling tell instead of size in Flush : " << pos);
    return pos; //Tell(fd)
    // return qfshelper->getSize(filename);
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
	/* setting the buffer to zero, to avoid garbage data */
    memset(buffer, 0, sizeof(Header));
    Header *header = new Header(-1);
    Read(buffer, sizeof(Header));
    header = (Header *)buffer;
	LOG4CXX_INFO(qfsfh_logger, "GetNextLogSize - " << filename << " - " << header->data_length);    
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
