#include "store.h"
#include "append_store.h"
#include <iostream>
#include "append_store_types.h"
// #include 

using namespace std;

int main() {

 cout << endl << "main entry point";
 
 StoreParameter sp = StoreParameter(); 
 sp.mPath = "//TEST//SIZE_64//TRY_25";
 sp.mAppend = false; 
 PanguAppendStore *pas = new PanguAppendStore(sp, false);
 string data_read;
 for(int i=1; i <= 16384; i++) {
  Handle h;
  h.mChunkId = 0;
  h.mIndex = i;
  pas->Read(h.ToString(), &data_read);
  cout << endl << "data_read : " << data_read.substr(0,50);
 }
 pas->Close();
}
