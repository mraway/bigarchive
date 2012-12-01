/*
 * > pu mkdir pangu://localcluster/test_cds/partitions
 * > ./cds_partitioner -i pangu://localcluster/test_cds -p pangu://localcluster/test_cds/partitions  -n 5
 *
 * generate dictionary data file and index file
 *
 */

#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <openssl/sha.h>
#include <vector>
#include <cassert>
#include "apsara/common/logging.h"
#include "apsara/common/timer.h"
#include <append/common_data_store.h>

using namespace std;
using namespace apsara;
using namespace apsara::AppendStore;
using namespace apsara::logging;

std::string tosha1(std::string key)
{
    unsigned char buf[20];
    SHA_CTX ctx;
    ::SHA1_Init(&ctx);
    ::SHA1_Update(&ctx, key.c_str(), key.size());
    ::SHA1_Final(buf, &ctx);

    return std::string((char*)&buf[0], 20);
}

std::string PrintableSha1(const std::string& sha1)
{
    static const char alphabet[] = "0123456789ABCDEF";

    assert(sha1.size() == 20);

    char hash_string[41];
    for (size_t i=0; i<20; ++i)
    {
        hash_string[2*i]   = alphabet[static_cast<uint8_t>(sha1[i]) / 16];
        hash_string[2*i+1] = alphabet[static_cast<uint8_t>(sha1[i]) % 16];
    }
    hash_string[40] = 0;

    return std::string((char*)&hash_string[0], 40);
}


void usage()
{
   cout <<"-i index_file_path -p partitioned_index_path -n number_of_partitions" << endl;
}

int main(int argc, char** argv)
{
    InitLoggingSystem();

    if (argc <= 1) 
    {
        usage();
        return 1;
    }

    std::string index_file;
    std::string partition_path;
    int no_partitions;

    char c;
    while ((c = getopt(argc, argv, "i:p:n:")) != -1)
    {
        switch (c) 
        {
        case 'i':
                index_file = optarg;
                break;
        case 'p':
                partition_path = optarg;
                break;
        case 'n':
                no_partitions = atoi(optarg);
                break;
        }
    }
    assert(no_partitions >= 1);

    CDSUtility cds(index_file);

    // partition the index
    //
    assert(GeneratePartitionIndex(index_file, no_partitions, partition_path));
    std::cout << "GeneratePartitionIndex() split index into " << no_partitions << " partitions." << std::endl;


    // go through all partitioned_index, read and compare the data
    //
    std::vector<int> count;
    for (int i = 0; i < no_partitions; ++i) 
    {
        count.push_back(0);
        CdsIndexReader creader(partition_path, i); 
        std::vector<CdsIndexRecord> subvec;
        while (creader.Next(subvec)) 
        {
            count[i] += subvec.size();
            for (uint32_t jj = 0; jj < subvec.size(); ++jj) 
            {
                std::string data;

                // read throughput from cds store
                uint64_t now = Timer::GetCurrentTimeInMicroSeconds();
                bool rdata = cds.Read(subvec[jj].mOffset, &data); 
                uint64_t latency = Timer::GetCurrentTimeInMicroSeconds() - now;
                assert(rdata);

                std::string shastring = tosha1(data);
                double readThroughput = data.size() * 1000000.0 / latency / (1024.0*1024.0);

                std::cout << shastring.compare(subvec[jj].mSha1Index) << "  Latency: " << latency << "microsec    Size: " << data.size() << " Byte" << endl;
                std::cout << "  Read Throughput: " << readThroughput << " MBps" << endl;
            }

            subvec.clear();
        }
    }

    int sum = 0;
    for (int i = 0; i < no_partitions; ++i) 
    {
        sum += count[i];
        std::cout << "  index partition " << i << " has " << count[i] << std::endl;
    }
}
