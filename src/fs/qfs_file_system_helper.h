#include "file_system_helper.h"
#include "libclient/KfsClient.h"

using std::string;

class QFSHelper : public FileSystemHelper {

	public:
		void Connect(); 
		void Connect(string metaserverhost, int metaserverport);
		bool IsFileExist(char *fname);
		bool IsDirectoryExist(char *dirname);
		int getSize(char *fname);
		int ListDir(char *pathname, vector<string> &result);
	public:	
	KfsClient *kfsClient;
};
