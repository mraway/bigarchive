#include "file_system_helper.h"
#include "libclient/KfsClient.h"

using std::string;

class QFSHelper : public FileSystemHelper {
	public:
		void Connect() {
			// connect to default host , port
			kfsClient = KFS::Connect(this, 40000);
		}

		void Connect(string metaserverhost, int metaserverport) {
			kfsClient = KFS::Connect(metaserverhost, metaserverport);
			if (! kfsClient ) {
				// logging_info("Connected to host and port ");
				// throw exception
			}
		}
	private:
	    KFS::KfsClient *kfsClient;
};
