/*
 * Prepare the cds:
 * 1. Generate the index and data file
 * 2. Inject the index to memcached
 * 3. Copy the data file to QFS
 */

#include <iostream>
#include "cds_index.h"
#include "../include/file_helper.h"
#include "../include/file_system_helper.h"
#include "../fs/qfs_file_system_helper.h"
#include "data_source.h"
#include "trace_types.h"
#include "snapshot_types.h"
#include "../common/timer.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

void usage(char* progname)
{
    cout << "Usage: " << progname << ": cds_name cds_file sample_file" << endl;
}

int main(int argc, char** argv)
{
    if (argc != 4) {
        usage(argv[0]);
        return -1;
    }
    DOMConfigurator::configure("Log4cxxConfig.xml");
    string cds_name(argv[1]);
    string cds_pathname(argv[2]);
    string sample_pathname(argv[3]);
    string qfs_cds_dir = "/cds";
    string qfs_cds_file = qfs_cds_dir + "/" + cds_name;

    DataSource source(cds_pathname, sample_pathname);
    CdsIndex cds;
    BlockMeta bm;
    uint64_t offset = 0;
    uint32_t bytes_written = 0;

    QFSHelper::Connect();

    if (FileSystemHelper::GetInstance()->IsDirectoryExists(qfs_cds_dir)) {
        FileSystemHelper::GetInstance()->RemoveDirectory(qfs_cds_dir);
    }
    FileSystemHelper::GetInstance()->CreateDirectory(qfs_cds_dir);

    FileHelper* fh = FileSystemHelper::GetInstance()->CreateFileHelper(qfs_cds_file, O_WRONLY);
    fh->Create();

    while (source.GetBlock(bm)) {
        MEASURE(cds.Set(bm.cksum_, offset));        // add to cds index cache
        MEASURE(bytes_written = fh->WriteData(bm.data_, bm.size_));	// write to qfs
        if (bytes_written != bm.size_)
            cout << "Error: write " << bytes_written << ", expect " << bm.size_ << endl;
        offset += bm.size_;
    }

    fh->Close();
    FileSystemHelper::GetInstance()->DestroyFileHelper(fh);
    TIMER_PRINT_ALL();
    exit(0);
}












