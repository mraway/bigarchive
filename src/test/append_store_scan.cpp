// test append store read and write capability
#include <unistd.h>

#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include "../append-store/append_store.h"
#include "../append-store/append_store_scanner.h"
#include "../fs/qfs_file_system_helper.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

LoggerPtr as_test_logger(Logger::getLogger("AppendStoreTest"));


// string random_data(int num_bytes) {
//     srand ( time(NULL) );
//     char result[num_bytes];
//     for(int i=0; i < num_bytes; i++)        {
//         result[i] = '!' + (rand() % 90); //(char) (rand() % 256); // all printable characters from 33 to 122
//     }       
//     return string(result);  

// }

PanguAppendStore* init_as_write(string& path)
{
    try {
        PanguAppendStore *pas = NULL;
        StoreParameter sp = StoreParameter(); ;
        sp.mPath = path;
        sp.mAppend = true;
        pas = new PanguAppendStore(sp, true);
        return pas;
    }
    catch (ExceptionBase& e) {
        LOG4CXX_ERROR(as_test_logger, "Couldn't init append store for write" << e.ToString());
    }
    catch (...) {
        LOG4CXX_ERROR(as_test_logger, "Couldn't init append store for write");
    }
    return NULL;
}

AppendStoreScanner* init_scan(string& path)
{
    try {
        AppendStoreScanner* scanner = new AppendStoreScanner(path,COMPRESSOR_LZO);
        //StoreParameter sp = StoreParameter();
        //sp.mPath = path;
        //sp.mAppend = false;	// read only
        return scanner;
    }
    catch (ExceptionBase& e) {
        LOG4CXX_ERROR(as_test_logger, "Couldn't init scanner for read" << e.ToString());
    }
    catch (...) {
        LOG4CXX_ERROR(as_test_logger, "Couldn't init scanner for read");
    }
    return NULL;
}


int main(int argc, char* argv[]) {
    DOMConfigurator::configure("Log4cxxConfig.xml");
    QFSHelper::Connect();
    string test_path("/astest");
    unsigned char data_char;
    char buf[256];
    vector<string> handles;
    uint64_t num_handle;

    LOG4CXX_INFO(as_test_logger, "-------------testing append store write--------------");
    PanguAppendStore* pas = init_as_write(test_path);
    for (unsigned char i = 1; i < 250; ++i)
    {
        data_char = i;
        memset(buf, data_char, i);
        string data(buf, i);
        string handle = pas->Append(data);
        handles.push_back(handle);
        memcpy((char*)&num_handle, handle.c_str(), sizeof(uint64_t));
        LOG4CXX_INFO(as_test_logger, "write return handle :" << num_handle);
    }
    pas->Flush();
    pas->Close();
    sleep(1);

    LOG4CXX_INFO(as_test_logger, "-------------testing append store scan--------------");
    AppendStoreScanner* scanner = init_scan(test_path);
    bool correctness = true;
    for (unsigned char i = 1; i < 250; ++i)
    {
        memcpy((char*)&num_handle, handles[i-1].c_str(), sizeof(uint64_t));
        LOG4CXX_INFO(as_test_logger, "try to read handle :" << num_handle);
        string data;
        bool res = scanner->Next(&handles[i-1], &data);
        LOG4CXX_INFO(as_test_logger, "read result: " << res << ", data length: " << data.length());
        bool data_correct = true;
        if (data.length() != i)
            data_correct = false;
        else {
            for (unsigned char j = 0; j < i; j++)
            {
                if (data[j] != (char)i)
                    data_correct = false;
            }
        }
        if (!data_correct)
            correctness = false;
        LOG4CXX_INFO(as_test_logger, "data verify result: " << data_correct);
    }
    delete scanner;



    LOG4CXX_INFO(as_test_logger, "append store correctness: " << correctness);
    sleep(1);

    return 0;
}













