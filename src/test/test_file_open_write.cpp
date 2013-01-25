
#include "exception.h"
#include <iostream>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"
#include <fcntl.h>
#include <cstring>
//
 using namespace std;


string random_data(int num_bytes) {
	srand ( time(NULL) );
	char result[num_bytes];
	for(int i=0; i < num_bytes; i++)	{
		result[i] = '!' + (rand() % 90); //(char) (rand() % 256); // all printable characters from 33 to 122
	}	
	return string(result);	

}


int main(int argc, char* argv[]) {
	cout << endl << "main entry point"; 
	clock_t start,end;
	time_t st, et;
	double total_time_1 = 0;
	double dif;
	int i, t;
	double total_time = 0;
	int MB = 0;
	long TOTAL_SIZE = 1024  * 1024;// * 1024 * MB;
	long CHUNK_SIZE = 2 * 1024; // 2048
	long NUM_CHUNKS = 0;
	long TRIES = 100;
	

	// cout << endl << argv[0];
	// cout << endl << argv[1];
	MB = atoi(argv[1]);

	TOTAL_SIZE *= MB;
	NUM_CHUNKS = TOTAL_SIZE / CHUNK_SIZE;

	string chunk_data = random_data(CHUNK_SIZE);
	cout << endl << "---------------------------------------------------------";
	//cout << endl << chunk_data;
	cout << endl << "---------------------------------------------------------";
	for(t = 0; t < TRIES; t++) {

		  QFSHelper *qfs = new QFSHelper();
  		  qfs->Connect();
			std::stringstream sstm;
			sstm << "//TEST_FS//SIZE_" << MB << "//TRY_" << t;
			string new_file_name = sstm.str();

			QFSFileHelper *qfsfh = new QFSFileHelper(qfs, new_file_name, O_WRONLY);
			qfsfh->Create();
		    	qfsfh->Open();
			start = clock();
			st = time(NULL);
			for(i = 0; i < NUM_CHUNKS; i++) {
				   int wrote = qfsfh->WriteData((char *)chunk_data.c_str(), chunk_data.size());
			}
		
		et = time(NULL);		
		end = clock();		
		dif = ((double(end - start) * 1000) / CLOCKS_PER_SEC);
		total_time += dif;
		//cout << endl << "Time taken for (" << new_file_name << ") : " << dif << " milli seconds ";
		cout << endl << (et - st);
		total_time_1 += (et - st);

		   qfsfh->Close();
		   //cout << endl << "data writtern and file closed";
  } 

	cout << endl << "total size is " << (TOTAL_SIZE / (1024 * 1024)) << " MB";
	cout << endl <<	"chunk size is " << CHUNK_SIZE;
	cout << endl << "num chunks is " << NUM_CHUNKS;

	cout << endl << "total time taken " << total_time;
	cout << endl << "average time to insert " << MB << "MB - " << (total_time / TRIES) << " milli seconds";
	cout << endl << "total time taken " << total_time_1;
	cout << endl << "average time to insert " << MB << "MB - " << (total_time_1 / TRIES) << " seconds";
	cout << endl;
}




/*
int main() {
  QFSHelper *qfs = new QFSHelper();
  qfs->Connect();
  string new_file_name;
  cout << endl << "enter a file name to write data : ";
  cin >> new_file_name;

  QFSFileHelper *qfsfh = new QFSFileHelper(qfs, new_file_name, O_WRONLY);
  if(qfs->IsFileExists(new_file_name)) {
   cout << endl << "File exists";
   qfsfh->Open();

   string name = "prakash";

   int fileSize = qfs->getSize(new_file_name);
   qfsfh->Seek(fileSize);

   int wrote = qfsfh->Write((char *)name.c_str(), name.size());

   cout << endl << "wrote " << wrote << " bytes";
   qfsfh->Close();
   cout << endl << "data writtern and file closed";
  } 
  else {
   cout << endl << "File doesnt exists";
  }
  cout << endl;
  qfs->DisConnect();
}
//
// src/test/test_qfsconnect.cpp
 // QFSFileHelper *qfsfh = new QFSFileHelper(qfs, "new_file", O_WRONLY);
 // qfsfh->Create();
 // cout << "Testing QFS host connection";
 // return 0;

*/
