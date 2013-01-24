#include "store.h"
#include "append_store.h"
#include <iostream>
#include <cstdlib>
#include <time.h>
#include "append_store_types.h"
#include "store.h"
#include "exception.h"
#include <iostream>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"
#include <fcntl.h>
#include <cstring>

using namespace std;

int main() {
	long CHUNK_SIZE = 4 * 1024; // 4 KB
	long FILE_SIZE = CHUNK_SIZE;
	long NUM_FOLDERS = 5;
	long NUM_FILES = 5 * 1024;
	long NUM_FILES_IN_FOLDER = 1024;
	long TOTAL_SIZE = NUM_FILES * FILE_SIZE;
	char *DATA = new char[CHUNK_SIZE];
	string ROOT_FOLDER = "BACKUPSTORE/FS";
	stringstream stream;
	string new_file_name;
	KFS::KfsClient *gKfsClient;
	int fd = -1;
	int i,j,k;

	if(TOTAL_SIZE != (FILE_SIZE * NUM_FOLDERS * NUM_FILES_IN_FOLDER)) {
		cout << endl << "something wrong";
		cout << endl << TOTAL_SIZE;
		cout << endl << FILE_SIZE * NUM_FOLDERS * NUM_FILES_IN_FOLDER;
		return 0;
	}
	
/*
	// Data Generation
	for(i=0; i < 10; i++) {
		for(j=0; j < CHUNK_SIZE; j++) {
			DATA[i][j] = '0' + i;
		}
	}
*/

	// File Write

	gKfsClient = KFS::Connect("128.111.46.96", 20000);

    if (!gKfsClient) {
		cerr << "kfs client failed to initialize...exiting" << "\n";
		exit(-1);
    }

/*
	for(i=0; i < NUM_FOLDERS; i++) {
		stream.str("");
		stream << ROOT_FOLDER << "/" << "FOLDER_" << i;
		gKfsClient->Mkdirs(stream.str().c_str());
		cout << endl << stream.str() << " directory created";
	}
*/

	for(i=0; i < NUM_FOLDERS; i++) {
		for(j=0; j < NUM_FILES_IN_FOLDER; j++) {

			stream.str("");
			stream << ROOT_FOLDER << "/" << "FOLDER_" << i << "/" << j;
			new_file_name = stream.str();

			// cout << endl << "new file name is " << new_file_name;

			if ((fd = gKfsClient->Open(new_file_name.c_str(), O_RDONLY)) < 0) {
    		   cout << "Open failed: " << KFS::ErrorCodeToStr(fd) << endl;
		        exit(-1);
	    	}

			// for(k = 0; k < NUM_CHUNKS; k++) {
				/* int res = */ gKfsClient->Read(fd, DATA, CHUNK_SIZE);
			// }
			cout << (*(DATA + 0));
	        gKfsClient->Close(fd);
		}
	}

}

		
