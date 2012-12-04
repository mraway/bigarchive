#include "qfs_file_helper.h"

	QFSFileHelper::QFSFileHelper(QFSHelper *qfshelper, string fname, int mode) {
		this->qfshelper = qfshelper;
		this->filename = fname;
		this->mode = mode;
		this->fd = -1;
	}

	void QFSFileHelper::Create()
	{
		fd = qfshelper->kfsClient->Create(fname.c_str());
		if (fd < 0) { }
	}

	void QFSFileHelper::Open() {
		fd = qfshelper->kfsClient->Open(fname, mode);
		if(fd < 0) { }
	}

	void QFSFileHelper::Close() {
		qfshelper->kfsClient->Close(fd);
	}

	void QFSFileHelper::Read(char *buffer, int length) {
		qfshelper->kfsClient->Read(fd, buffer, length);
	}

	void QFSFileHelper::Write(char *buffer, int length) {
		qfshelper->kfsClient->Write(fd, buffer, length);
	}

	void QFSFileHelper::Seek(int offset) {
		qfshelper->kfsClient->Seek(fd, offset);
	}

