
#include "exception.h"
#include <iostream>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"
#include <fcntl.h>
#include <cstring>

using namespace std;


string random_data(int num_bytes) {
	srand ( time(NULL) );
	char result[num_bytes];
	for(int i=0; i < num_bytes; i++)	{
		result[i] = '!' + (rand() % 90); 
	}	
	return string(result);	
}


int main(int argc, char* argv[]) {

	cout << endl << "main entry point"; 
	double total_time_1 = 0;
	int i, t;
	int MB = 0;
	long TOTAL_SIZE = 0;
	long CHUNK_SIZE = 2 * 1024; 
	long NUM_CHUNKS = 0;
	long TRIES = 100;
 	int fd;	

	MB = atoi(argv[1]);

	TOTAL_SIZE = MB * 1024  * 1024;
	NUM_CHUNKS = TOTAL_SIZE / CHUNK_SIZE;

	string chunk_data = random_data(CHUNK_SIZE);
	
	KFS::KfsClient *gKfsClient;
	gKfsClient = KFS::Connect("128.111.46.96", 20000);

    if (!gKfsClient) {
		cerr << "kfs client failed to initialize...exiting" << "\n";
		exit(-1);
    }	

	for(t = 0; t < TRIES; t++) {	

		std::stringstream sstm;
		sstm << "//TEST_FS//SIZE_" << MB << "//TRY_" << t;
		string new_file_name = sstm.str();

		if ((fd = gKfsClient->Create(new_file_name.c_str())) < 0) {
    		   cout << "Create failed: " << KFS::ErrorCodeToStr(fd) << endl;
		        exit(-1);
    	}


		for(i = 0; i < NUM_CHUNKS; i++) {
			int res = gKfsClient->Write(fd, chunk_data.c_str(), chunk_data.size());
		}
	
        gKfsClient->Sync(fd);
    
        gKfsClient->Close(fd);

  	} 

	cout << endl << "total size is " << (TOTAL_SIZE / (1024 * 1024)) << " MB";
	cout << endl <<	"chunk size is " << CHUNK_SIZE;
	cout << endl << "num chunks is " << NUM_CHUNKS;

}




