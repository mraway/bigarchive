

		void QFSHelper::Connect() 
		{
			Conect(string("localhost"), 40000);
		}

		void QFSHelper::Connect(string metaserverhost, int metaserverport) 
		{
			kfsClient = KFS::Connect(metaserverhost, metaserverport);
			if (! kfsClient ) {
				// logging_info("Connected to host and port ");
				// throw exception
			}
		}

		bool QFSHelper::IsFileExist(char *fname)
		{
		 return kfsClient->Exists(fname);
		}

		bool QFSHelper::IsDirectoryExist(char *dirname)
		{
		 return kfsClient->Exists(dirname);
		}

		int QFSHelper::getSize(char *fname)
		{
			KFS::KfsFileAttr kfsattr = new KfsFileAttr(); // Stat method takes reference of this object
			kfsClient->Stat(fname, kfsattr, true); // true is for computing the size
			return kfsattr.fileSize;
		}

		int QFSHelper::ListDir(char *pathname, vector<string> &result)
		{
			return kfsClient->ReadDir(pathname, result);
		}

