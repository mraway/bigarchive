#include "file_system_helper.h"
// include issues
// #include "libclient/KfsClient.h"

using std::string;

class QFSHelper : public FileSystemHelper {
	public:
		void Connect() {
			Conect(string("localhost"), 40000);
		}

		void Connect(string metaserverhost, int metaserverport) {
			kfsClient = KFS::Connect(metaserverhost, metaserverport);
			if (! kfsClient ) {
				// logging_info("Connected to host and port ");
				// throw exception
			}
		}

		bool IsFileExist(char *fname)
		{
		 //KFS::KfsClient *kfsClient = NULL;
		 //kfsClient = KFS::Connect(serverHost, port);
		 return kfsClient->Exists(fname);
		}

		bool IsDirectoryExist(char *dirname)
		{
		 //KFS::KfsClient *kfsClient = NULL;
		 //kfsClient = KFS::Connect(serverHost, port);
		 return kfsClient->Exists(dirname);
		}

		int getSize(char *fname)
		{
			KFS::KfsFileAttr kfsattr = new KfsFileAttr(); // Stat method takes reference of this object
			kfsClient->Stat(fname, kfsattr, true); // true is for computing the size
			return kfsattr.fileSize;
		}

	    KFS::KfsClient *kfsClient;
};
