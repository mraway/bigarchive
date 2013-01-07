#include "exception.h"
// #include <iostream>
#include "qfs_file_system_helper.h"
#include "qfs_file_helper.h"
#include <fcntl.h>

using namespace std;

int main() {
 QFSHelper *qfs = new QFSHelper();
 qfs->Connect();
 cout << endl << "Connected to localhost:40000";
 qfs->DisConnect();
 cout << endl << "DisConnected";
 cout << endl;
}
