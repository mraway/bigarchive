#include "snapshot_types.h"
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
    BlockMeta bm;
    SegmentMeta sm;
    SnapshotMeta ssm;

    cout << bm.handle_ << sm.handle_ << ssm.size_ << endl;
    return 0;
}










