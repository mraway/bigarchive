#include "exception.h"
#include <iostream>
// #include "qfs_file_system_helper.h"
#include "KfsClient.h"
#include "KfsAttr.h"

using namespace std;

int main() {
 // QFSHelper *qfs = new QFSHelper();
 // qfs->Connect();
 KFS::KfsClient *gKfsClient; 
 gKfsClient = KFS::Connect("localhost", 20000);
 cout << "Testing QFS host connection";
 
 int res = gKfsClient->Mkdirs("home_prakash");
 if (res < 0 && res != -EEXIST) {
  cout << "Mkdir failed: " << KFS::ErrorCodeToStr(res) << endl;
  exit(-1);
 }
 cout << "result is : " << res;
 cout << "done !!";

 // return 0;
}
