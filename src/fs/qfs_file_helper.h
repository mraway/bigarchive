#include "file_helper.h"
#include "file_system_helper.h"
#include "qfs_file_system_helper.h"

class QFSFileHelper : public FileHelper {
 public:
	QFSFileHelper(QFSHelper *qfshelper, string fname, int mode);
	void Create();
	void Open();
	void Close();
	void Read(char *buffer, int length);
	void Write(char *buffer, int length);
	void Seek(int offset);
 private:
	QFSHelper *qfshelper;
};
