#include "../snapshot/data_source.h"
#include <fstream>
#include "socketstream.h"

using namespace std;
using namespace galik;

int main(int argc, char** argv)
{
    if (argc != 4) {
        cout << "Usage: " << argv[0] << " trace_file sample_data output" << endl;
        return -1;
    }
    string trace(argv[1]);
    string sample_data(argv[2]);
    DataSource source(trace, sample_data);
    SegmentMeta sm;
    string output_file(argv[3]);
    ofstream os(output_file.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
    while (source.GetSegment(sm)) {
        for (size_t i = 0; i < sm.segment_recipe_.size(); ++i)
        {
            os.write(sm.segment_recipe_[i].data_, sm.GetBlockSize(i));
        }
    }
    os.close();
}
















