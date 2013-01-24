#include "exception.h"
#include <iostream>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"
#include <fcntl.h>
//
using namespace std;
// //
 int main() {
   QFSHelper *qfs = new QFSHelper();
   qfs->Connect();
   string new_dir_name;
   cout << endl << "enter a new directory name to create : ";
   cin >> new_dir_name;
   if(qfs->IsDirectoryExists(new_dir_name)) {
    cout << endl << "directory already exists ";
    return 0;
   }
   cout << endl << "IsDirectory Exists : before creation : " << qfs->IsDirectoryExists(new_dir_name);
   qfs->CreateDirectory(new_dir_name);
   cout << endl << "IsDirecotry Exists : after creation  : " << qfs->IsDirectoryExists(new_dir_name);
   return 0;
 }
           
