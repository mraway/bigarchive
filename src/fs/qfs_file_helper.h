#include "file_helper.h"
#include "file_system_helper.h"
#include "qfs_file_system_helper.h"
#include "exception.h"
#include <fcntl.h>
#include <stdlib.h>

class QFSFileHelper : public FileHelper {
 public:
	QFSFileHelper(QFSHelper *qfshelper, string fname, int mode);
	void Create();
	void Open();
	void Close();
	int Read(char *buffer, int length);
	int Write(char *buffer, int length);
	void Seek(int offset);
	int GetNextLogSize();
 private:
	QFSHelper *qfshelper;
};
