#include<file_helper.h>
#include<file_system_helper.h>

class QFSFileHelper : public FileHelper {
 public:
	QFSFileHelper(QFSHelper qfshelper, string fname, int mode) {
		this->qfshelper = qfshelper;
		this->filename = fname;
		this->mode = mode;
		this->fd = -1;
	}

	void Create()
	{
		fd = qfshelper.kfsClient->Create(fname.c_str());
		if (fd < 0) { }
	}

	void Open() {
		fd = qfshelper.kfsClient->Open(fname, mode);
		if(fd < 0) { }
	}

	void Close() {
		qfshelper.kfsClient->Close(fd);
	}

	void Read(char *buffer, int length) {
		qfshelper.kfsClient->Read(fd, buffer, length);
	}

	void Write(char *buffer, int length) {
		qfshelper.kfsClient->Write(fd, buffer, length);
	}

	static bool IsFileExist(char *fname)
	{
	 KFS::KfsClient *kfsClient = NULL;
	 kfsClient = KFS::Connect(serverHost, port);
	 return qfshelper.kfsClient->Exists(fname);
	}

	static bool IsDirectoryExist(char *dirname)
	{
	 KFS::KfsClient *kfsClient = NULL;
	 kfsClient = KFS::Connect(serverHost, port);
	 return qfshelper.kfsClient->Exists(fname.c_str());
	}

 private:
	QFSHelper qfshelper;
};
