#include "bloom_filter.h"
#include "bloom_filter_functions.h"
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
    BloomFilter<Checksum> bf(100, 0.1, kBloomFilterFunctions, 4);
    cout << bf.GetCurrentTotalBits() << endl;
    for (int i = 0; i < 256; i ++) {
        Checksum tmp;
        tmp.data_[0] = i;
        cout << bf.Exist(tmp);
        bf.AddElement(tmp);
        cout << " " << bf.Exist(tmp) << endl;
    }
    for (int i = 0; i < 256; ++i)
    {
        Checksum tmp;
        tmp.data_[2] = i;
        cout << " " << bf.Exist(tmp);
    }
    exit(0);
}
