#include "file_helper.h"
#include "file_system_helper.h"
#include "qfs_file_system_helper.h"
#include "exception.h"
#include <fcntl.h>
#include <stdlib.h>
#include <cstring>

class QFSFileHelper : public FileHelper {
 public:
	QFSFileHelper(QFSHelper *qfshelper, string fname, int mode);
	void Create();
	void Open();
	void Close();
	int Read(char *buffer, int length);
	int Write(char *buffer, int length);
	int Flush(char *buffer, int length);
	void Seek(int offset);
	uint32_t GetNextLogSize();
 private:
	QFSHelper *qfshelper;
};


struct Header {
 Header(int len) : data_length(len) {}
 int data_length;
};
