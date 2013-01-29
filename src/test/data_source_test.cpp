#include "../snapshot/data_source.h"
#include <fstream>
#include "socketstream.h"

using namespace std;
using namespace galik;

int main(int argc, char** argv)
{
    string trace("/mnt/data2/scanlog/win2003_64/data/VM-2C0A05C1.1000-19189-8071-merged.vhd.bv4");
    string sample_data("sample_data");

    DataSource source(trace, sample_data);
    SegmentMeta sm;

    /*
    string output_file("data_source_test.output");
    ofstream os(output_file.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
    while (source.GetSegment(sm)) {
        for (size_t i = 0; i < sm.block_list_.size(); ++i)
        {
            os.write(sm.block_list_[i].data_, sm.GetBlockSize(i));
        }
    }
    os.close();
    */
    net::socketstream ss;
    ss.open("localhost", 11863);
    uint16_t put = 0;
    uint32_t blksize = 0;
    while (source.GetSegment(sm)) {
        for (size_t i = 0; i < sm.block_list_.size(); ++i)
        {
            ss.write((char*)&put, sizeof(put));
            ss.write((char*)sm.block_list_[i].cksum_, CKSUM_LEN);
            blksize = sm.GetBlockSize(i); 
            ss.write((char*)&blksize, sizeof(blksize));
            ss.write(sm.block_list_[i].data_, blksize);
        }
    }

    ss.close();
}
















