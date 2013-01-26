#include "data_source.h"
#include <fstream>
using namespace std;

int main(int argc, char** argv)
{
    string trace("/mnt/data2/scanlog/win2003_64/data/VM-2C0A05C1.1000-19189-8071-merged.vhd.bv4");
    string sample_data("sample_data");
    string output_file("data_source_test.output");
    ofstream os(output_file.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
    DataSource source(trace, sample_data);
    SegmentMeta sm;
    while (source.GetSegment(sm)) {
        for (size_t i = 0; i < sm.block_list_.size(); ++i)
        {
            os.write(sm.block_list_[i].data_, sm.GetBlockSize(i));
        }
    }
    os.close();
}
