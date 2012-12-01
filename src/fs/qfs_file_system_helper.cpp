#include "file_system_helper.h"
// include issues
#include "libclient/KfsClient.h"

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
	    KFS::KfsClient *kfsClient;
};
