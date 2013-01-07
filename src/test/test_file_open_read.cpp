
#include "exception.h"
#include <iostream>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"
#include <fcntl.h>
//
 using namespace std;
//
int main() {
  QFSHelper *qfs = new QFSHelper();
  qfs->Connect();
  string new_file_name;
  cout << endl << "enter a file name to read data : ";
  cin >> new_file_name;

  QFSFileHelper *qfsfh = new QFSFileHelper(qfs, new_file_name, O_RDONLY);

  if(qfs->IsFileExists(new_file_name)) {
   cout << endl << "File exists";
   qfsfh->Open();
   uint32_t size = qfsfh->GetNextLogSize(); // char *data = new char[11]; 
   cout << endl << "GetNextLogSize : " << size;

   char *data = new char[size + 1];
   qfsfh->Read(data, size);
   data[size] = '\0';
   cout << endl << "data read : " << data;
   cout << endl << "get next log size " << size;
   qfsfh->Close();
   cout << endl << "data read and file closed";
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
