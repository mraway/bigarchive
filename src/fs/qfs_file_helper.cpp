#include "qfs_file_helper.h"

#include <iostream>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// static logger variable
LoggerPtr qfsfh_logger(Logger::getLogger( "appendstore.qfs_helper"));


QFSFileHelper::QFSFileHelper(QFSHelper *qfshelper, string fname, int mode) {
    DOMConfigurator::configure("/home/prakash/log_config.xml");
    LOG4CXX_INFO(qfsfh_logger, "File helper for file created : " << fname );
    this->qfshelper = qfshelper;
    this->filename = fname;
    this->mode = mode;
    this->fd = -1;
}

void QFSFileHelper::Create()
{
    fd = qfshelper->kfsClient->Create(filename.c_str());
    if (fd < 0) { 
	LOG4CXX_ERROR(qfsfh_logger, "File Creation failed : " << filename);
	THROW_EXCEPTION(FileCreationException, "Failed while creating file : " + filename);
    }
}

void QFSFileHelper::Open() {
    fd = qfshelper->kfsClient->Open(filename.c_str(), mode);
    if(fd < 0) { 
	LOG4CXX_ERROR(qfsfh_logger, "Failed while opening file : " << filename << ", ERROR :" << KFS::ErrorCodeToStr(fd));
	THROW_EXCEPTION(FileOpenException, "Failed while opening file : " + filename + " ERROR : " + KFS::ErrorCodeToStr(fd));
    }
}

void QFSFileHelper::Close() {
    // qfshelper->kfsClient->Sync(fd);
    qfshelper->kfsClient->Close(fd);
}

int QFSFileHelper::Read(char *buffer, int length) {
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
    int dataLength = length + sizeof(Header);
    Header header(length);

    string data(dataLength, 0);
    memcpy(&data[0], &header, sizeof(Header));
    memcpy(&data[sizeof(Header)], buffer, length);

    int bytes_wrote = qfshelper->kfsClient->Write(fd, data.c_str(), dataLength);
    
    // std::cout << bytes_wrote << " " << dataLength; 

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
    return bytes_wrote;
}

void QFSFileHelper::Seek(int offset) {
    qfshelper->kfsClient->Seek(fd, offset);
}

uint32_t QFSFileHelper::GetNextLogSize() {
    char *buffer = new char[sizeof(Header)];
    Header *header = new Header(-1);
    Read(buffer, sizeof(Header));
    header = (Header *)buffer;
    return header->data_length;
}





