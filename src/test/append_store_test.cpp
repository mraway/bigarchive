#include "store.h"
#include "append_store.h"
#include <iostream>
#include <cstdlib>
#include <time.h>
#include "append_store_types.h"
#include "store.h"
/*
#include <boost/chrono.hpp>
#include <boost/thread.hpp>


int main()
{
  boost::chrono::high_resolution_clock::time_point start = boost::chrono::high_resolution_clock::now();
  boost::this_thread::sleep(boost::posix_time::millisec(2000));
  boost::chrono::milliseconds ms = boost::chrono::duration_cast<boost::chrono::milliseconds> (boost::chrono::high_resolution_clock::now() - start);
  std::cout << "2000ms sleep took " << ms.count() << "ms " << "\n";
  return 0;
}
*/

using namespace std;



string random_data(int num_bytes) {
	srand ( time(NULL) );
	char result[num_bytes];
	for(int i=0; i < num_bytes; i++)	{
		result[i] = '!' + (rand() % 90); //(char) (rand() % 256); // all printable characters from 33 to 122
	}	
	return string(result);	

}

/*
const clock_t begin_time = clock();
// do something
std::cout << float( clock () - begin_time ) /  CLOCKS_PER_SEC;
*/

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
	long CHUNK_SIZE = 4096; // 2048
	long NUM_CHUNKS = 0;
	long TRIES = 1;

	st = time(NULL);
	// cout << endl << argv[0];
	// cout << endl << argv[1];
	MB = atoi(argv[1]);

	TOTAL_SIZE *= MB;
	NUM_CHUNKS = TOTAL_SIZE / CHUNK_SIZE;

	string chunk_data = random_data(CHUNK_SIZE);
	cout << endl << "---------------------------------------------------------";
	cout << endl << chunk_data;
	cout << endl << "---------------------------------------------------------";
	for(t = 0; t < TRIES; t++) {
		StoreParameter sp = StoreParameter(); 
		std::stringstream sstm;
		sstm << "//TESTREAD//SIZE_" << MB << "//TRY_" << t;
		string store_name = sstm.str();
		sp.mPath = store_name;
		sp.mAppend = true;
		DataFileCompressionFlag dfcf = COMPRESSOR_LZO;
		sp.mCompressionFlag = dfcf;//COMPRESSOR_LZO;
		st = time(NULL);		
		start = clock();
		PanguAppendStore *pas = new PanguAppendStore(sp, true);
		for(i = 0; i < NUM_CHUNKS; i++) {
			// chunk_data[0] = 'a' + (i % 26);
			string h1_str = pas->Append(chunk_data);
			if(i == (NUM_CHUNKS - 1)) {
				Handle h1(h1_str);
				cout << "(" << h1.mChunkId << ":" << h1.mIndex << ")";
			}
		}
		
		pas->Flush();
		pas->Close();
		et = time(NULL);		
		end = clock();		
		dif = ((double(end - start) * 1000) / CLOCKS_PER_SEC);
		total_time += dif;
		cout << endl << "Time taken for (" << store_name << ") : " << dif << " milli seconds ";
		cout << endl << "Time taken " << (et - st) << " seconds";
		total_time_1 += (et - st);
	}
	cout << endl << "total size is " << (TOTAL_SIZE / (1024 * 1024)) << " MB";
	cout << endl <<	"chunk size is " << CHUNK_SIZE;
	cout << endl << "num chunks is " << NUM_CHUNKS;

	cout << endl << "total time taken " << total_time;
	cout << endl << "average time to insert " << MB << "MB - " << (total_time / TRIES) << " milli seconds";
	cout << endl << "total time taken " << total_time_1;
	cout << endl << "average time to insert " << MB << "MB - " << (total_time_1 / TRIES) << " seconds";
	cout << endl;
    /*
	StoreParameter sp = StoreParameter(); 
	sp.mPath = "/root/store06";
	sp.mAppend = true;
	PanguAppendStore *pas = new PanguAppendStore(sp, true);
	string data("my name is billa"); 
	string handle = pas->Append(data);
	cout << endl << "data wrote : " << data;
	cout << endl << "handle is : " << handle;
	pas->Flush();
	pas->Close();	 
	cout << endl << "file closed ";
	 sp.mAppend = false;
	 PanguAppendStore *pas_read = new PanguAppendStore(sp, false);
	 cout << endl << "read store opened ";
	 string data_read;
	 for(int i = 0 ; i < 5; i++) {
	  Handle h;
	  h.mChunkId = 0;
	  h.mIndex = i;
	  pas_read->Read(h.ToString(), &data_read);
	  cout << endl << "data_read : " << data_read;
	 }
	 pas_read->Flush();
	 pas_read->Close();
	
	 cout << endl << "end of main";
	 
	 cout << endl;
	*/
}



