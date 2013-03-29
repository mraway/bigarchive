// test concurrent read/write to a single QFS file

#include "KfsClient.h"
#include "KfsAttr.h"
#include <iostream>
#include <string>
#include <fcntl.h>

using namespace std;

#define DATA_BUF_SIZE (1024 * 1024)

int main(int argc, char** argv)
{
    // init
    KFS::KfsClient *pfs;
    pfs = KFS::Connect("127.0.0.1", 20000);
    if (!pfs) {
        cout << "unable to connect to QFS" << endl;
        return -1;
    }
    char* data = new char[DATA_BUF_SIZE];    
    char* rdbuf = new char[DATA_BUF_SIZE];
    int rdfd = -1, wrfd = -1, res = 0;

    /*
    string rw_file("/qfstest.rw");
    wrfd = pfs->Open(rw_file.c_str(), O_RDWR | O_CREAT | O_TRUNC);
    if (wrfd < 0) {
        cout << "RW open failed: " << KFS::ErrorCodeToStr(wrfd));
        return -1;
    }
    for (int i = 0; i < 100; i++) {
        res = pfs->Write(wrfd, data, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "write failed: " << KFS::ErrorCodeToStr(res));
            return -1;
        }
        cout << "write number bytes: " << res << ", current position is: " << pfs->Tell(wrfd));
    }
    pfs->Seek(wrfd, 0);
    cout << "writter seek to " << pfs->Tell(wrfd));
    for (int i = 0; i < 100; ++i)
    {
        res = pfs->Read(wrfd, rdbuf, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "read failed: " << KFS::ErrorCodeToStr(res));
            return -1;
        }
        cout << "read number bytes: " << res << ", current position is: " << pfs->Tell(wrfd));
    }
    pfs->Close(wrfd);
    */
    /*
    // append mode test
    string append_file("/qfstest.append");
    cout << "QFS chunk size is " << pfs->GetChunkSize(append_file.c_str()));
    // open in append mode
    wrfd = pfs->Open(append_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND);
    if (wrfd < 0) {
        cout << "writter open failed: " << KFS::ErrorCodeToStr(wrfd));
        return -1;
    }
    // append some data
    for (int i = 0; i < 10; i++) {
        res = pfs->RecordAppend(wrfd, data, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "write failed: " << KFS::ErrorCodeToStr(res));
            return -1;
        }
        cout << "write number bytes: " << res << ", current position is: " << pfs->Tell(wrfd));
    }
    pfs->Close(wrfd);
    // show append file size
    KFS::KfsFileAttr kfsattr; 
    pfs->Stat(append_file.c_str(), kfsattr); 
    cout << "file size of " << append_file << " is " << kfsattr.fileSize);
    // open and write again
    wrfd = pfs->Open(append_file.c_str(), O_WRONLY | O_APPEND);
    for (int i = 0; i < 54; i++) {
        res = pfs->RecordAppend(wrfd, data, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "write failed: " << KFS::ErrorCodeToStr(res));
            return -1;
        }
        cout << "write number bytes: " << res << ", current position is: " << pfs->Tell(wrfd));
    }
    // look file size one more time
    pfs->Stat(wrfd, kfsattr); 
    pfs->Close(wrfd);
    cout << "file size of " << append_file << " is " << kfsattr.fileSize);
    */

    // 1. create a file and fill 100MB data
    string write_file("/qfstest.write");
    cout << "QFS chunk size is " << pfs->GetChunkSize(write_file.c_str()) << endl;
    wrfd = pfs->Open(write_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC);
    if (wrfd < 0) {
        cout << "writter open failed: " << KFS::ErrorCodeToStr(wrfd) << endl;
        return -1;
    }
    for (int i = 0; i < 100; i++) {
        res = pfs->Write(wrfd, data, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "write failed: " << KFS::ErrorCodeToStr(res) << endl;
            return -1;
        }
        cout << "write number bytes: " << res << ", current position is: " << pfs->Tell(wrfd) << endl;
    }
    pfs->Close(wrfd);
    // 2. open file and read 100MB data
    rdfd = pfs->Open(write_file.c_str(), O_RDONLY);
    if (rdfd < 0) {
        cout << "reader open failed: " << KFS::ErrorCodeToStr(wrfd) << endl;
        return -1;
    }
    for (int i = 0; i < 100; ++i)
    {
        res = pfs->Read(rdfd, rdbuf, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "read failed: " << KFS::ErrorCodeToStr(res) << endl;
            return -1;
        }
        cout << "read number bytes: " << res << ", current position is: " << pfs->Tell(rdfd) << endl;
    }
    // 3. continue write, but rdfd is still open
    wrfd = pfs->Open(write_file.c_str(), O_WRONLY);
    if (wrfd < 0) {
        cout << "writter open failed: " << KFS::ErrorCodeToStr(wrfd) << endl;
        return -1;
    }
    // seek to end
    pfs->Seek(wrfd, 0, SEEK_END);
    cout << "writter seek to " << pfs->Tell(wrfd) << endl;
    for (int i = 0; i < 10; i++) {
        res = pfs->Write(wrfd, data, DATA_BUF_SIZE);
        if (res < 0) {
            cout << "write failed: " << KFS::ErrorCodeToStr(res) << endl;
            return -1;
        }
        cout << "write number bytes: " << res << ", current position is: " << pfs->Tell(wrfd) << endl;
    }

    pfs->Close(wrfd);
    pfs->Close(rdfd);
    delete[] rdbuf;
    delete[] data;
    return 0;
}















