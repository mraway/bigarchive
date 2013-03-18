
#ifndef QFS_FILE_HELPER_H
#define QFS_FILE_HELPER_H

#include "../include/file_helper.h"
#include "../include/exception.h"
#include "qfs_file_system_helper.h"
#include <cstring>

class QFSFileHelper : public FileHelper {
public:
	QFSFileHelper(QFSHelper *qfshelper, string fname, int mode);
	void Create();
	void Open();
	void Close();
	int Read(char *buffer, int length);
	int Write(char *buffer, int length);
	int WriteData(char *buffer, int length);
	int Flush(char *buffer, int length);
	int FlushData(char *buffer, int length);
    int Append(char *buffer, int length);
	void Seek(int offset);
	uint32_t GetNextLogSize();
private:
    /*
     * store a pointer to the file system helper instance,
     * this is just for convenience because we use a lot of qfs client APIs
     */
	QFSHelper *qfshelper;
	string get_mode();
};


struct Header {
    Header(uint32_t len) : data_length(len) {}
    uint32_t data_length;
};

#endif
