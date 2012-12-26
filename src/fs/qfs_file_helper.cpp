#include "qfs_file_helper.h"


	QFSFileHelper::QFSFileHelper(QFSHelper *qfshelper, string fname, int mode) {
		LOG4CXX_INFO(logger, "File helper for file created : " << fname );
		this->qfshelper = qfshelper;
		this->filename = fname;
		this->mode = mode;
		this->fd = -1;
	}

	void QFSFileHelper::Create()
	{
		fd = qfshelper->kfsClient->Create(filename.c_str());
		if (fd < 0) { 
			LOG4CXX_ERROR(logger, "File Creation failed : " << filename);
			THROW_EXCEPTION(FileCreationException, "Failed while creating file : " + filename);
		}
	}

	void QFSFileHelper::Open() {
		fd = qfshelper->kfsClient->Open(filename.c_str(), mode);
		if(fd < 0) { 
			LOG4CXX_ERROR(logger, "Failed while opening file : " << filename << ", ERROR :" << KFS::ErrorCodeToStr(fd));
			THROW_EXCEPTION(FileOpenException, "Failed while opening file : " + filename + " ERROR : " + KFS::ErrorCodeToStr(fd));
		}
	}

	void QFSFileHelper::Close() {
		// flushes out all changes
		// qfshelper->kfsClient->Sync(fd);
		// closes the file handle
		qfshelper->kfsClient->Close(fd);
	}

	int QFSFileHelper::Read(char *buffer, int length) {
		int bytes_read = qfshelper->kfsClient->Read(fd, buffer, length);
		if(bytes_read != length) {
			if(bytes_read < 0) {
				LOG4CXX_ERROR(logger, "Failure while reading file " << filename << " ERROR : " << KFS::ErrorCodeToStr(bytes_read));
				THROW_EXCEPTION(AppendStoreReadException, "Failed while reading file " + filename + " ERROR : " + KFS::ErrorCodeToStr(bytes_read));
			}
			else {
				LOG4CXX_ERROR(logger, "Less number of bytes read from file than specified")
			}
		}
		return bytes_read;	
	}

	int QFSFileHelper::Write(char *buffer, int length) {
		

/*
 int dataLength = size + sizeof(LogItemTag);
 LogItemTag logTag(size);
 logTag.reserved = logTag.length;

 string data(dataLength, 0);
 memcpy(&data[0], &logTag, sizeof(LogItemTag));
 memcpy(&data[sizeof(LogItemTag)], logBuf, size);
 */
		int bytes_wrote = qfshelper->kfsClient->Write(fd, buffer, length);
		if( bytes_wrote != length) {
			string bytes_wrote_str = "" + bytes_wrote;
			string length_str = "" + length;
			LOG4CXX_ERROR(logger, "Was able to write only " << bytes_wrote << ", instead of " << length);
			THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write only " + bytes_wrote_str + ", instead of " + length_str);
		}
	}


	int QFSFileHelper::Flush(char *buffer, int length) {
                int bytes_wrote = qfshelper->kfsClient->Write(fd, buffer, length);
                if( bytes_wrote != length) {
                        string bytes_wrote_str = "" + bytes_wrote;
                        string length_str = "" + length;
                        LOG4CXX_ERROR(logger, "Was able to write(flush) only " << bytes_wrote << ", instead of " << length);
                        THROW_EXCEPTION(AppendStoreWriteException,  "Was able to write(flush) only " + bytes_wrote_str + ", instead of " + length_str);
                }
		qfshelper->kfsClient->Sync(fd);
        }

	void QFSFileHelper::Seek(int offset) {
		qfshelper->kfsClient->Seek(fd, offset);
	}

	int QFSFileHelper::GetNextLogSize() {
		return -1;
        }





