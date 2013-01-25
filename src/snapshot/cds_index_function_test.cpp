#include "cds_index.h"

int main(int argc, char **argv)
{
    DOMConfigurator::configure("Log4cxxConfig.xml");
    string mc_options("--SERVER=128.111.46.222:11211 --BINARY-PROTOCOL");

    Block blks[5];
    for (int i = 0; i < 5; ++i)
    {
        blks[i].cksum_[0] = 'a' + i;
        blks[i].offset_ = i;
    }

    CdsIndex cds(mc_options);
    cds.Set(blks[2].cksum_, blks[2].offset_);
    cds.Set(blks[4].cksum_, blks[4].offset_);

    for (int i = 0; i < 5; ++i)
    {
        cout << cds.Get(blks[i].cksum_, &(blks[i].offset_)) << endl;
    }

    char *cksums[5];
    bool results[5];
    uint64_t offsets[5];
    for (int i = 0; i < 5; ++i)
    {
        cksums[i] = (char*)(blks[i].cksum_);
    }
    cds.BatchGet(cksums, 5, results, offsets);
    for (int i = 0; i < 5; ++i)
    {
        cout << results[i] << " " << offsets[i] << endl;
    }
    return 0;
}










