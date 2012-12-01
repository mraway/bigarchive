#include<file_helper.h>
#include<file_system_helper.h>

class QFSFileHelper : public FileHelper {
 public:
	QFSFileHelper(QFSHelper fshelper, string fname, int mode) {
		this->fshelper = fshelper;
		this->filename = fname;
		this->mode = mode;
		this->fd = -1;
	}

	void Open() {
		fd = fshelper->Open(fname, mode);
		if(fd < 0) {
			// logging
			// throw exception
		}
	}

	void Close() {
		fshelper->Close(fd);
	}

	void Read(char *buffer, int length) {
		// check for buffer -> null
		fshelper->Read(fd, buffer, length);
	}

	void Write(char *buffer, int length) {
		fshelper->Write(fd, buffer, length);
	}
 private:
	FileSystemHelper fshelper;
};
