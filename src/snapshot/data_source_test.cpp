#include "data_source.h"
#include <fstream>
using namespace std;

int main(int argc, char** argv)
{
    string trace("");
    string sample_data("");
    string output_file("data_source_test.output");
    ofstream os(output_file.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
    DataSource source(trace, sample_data);
    SegmentMeta sm;
    while (source.GetSegment(sm)) {
        for (int i = 0; i < sm.block_list_.size(); ++i)
        {
            os.write(sm.block_list_[i].data_, sm.GetBlockSize(i));
        }
    }
    os.close();
}
