
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
  cout << endl << "enter a new file name : ";
  cin >> new_file_name;

  QFSFileHelper *qfsfh = new QFSFileHelper(qfs, new_file_name, O_WRONLY);
  if(qfs->IsFileExists(new_file_name)) {
   cout << endl << "File Already exists";
  } 
  else {
   cout << endl << "File not found, Creating new one";
   qfsfh->Create();
   cout << endl << "IsFileExists : " << qfs->IsFileExists(new_file_name);
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
