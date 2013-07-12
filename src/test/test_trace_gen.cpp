// Generate some short test traces for snapshot read/write
#include <string>
#include <fstream>
#include "../snapshot/trace_types.h"

const uint32_t VM_SIZE = 6 * 1024 * 1024;

using namespace std;

int main(int argc, char** argv)
{
    string vm_id = "vm-test";
    string ss_id_prefix = "1-1-";
    for (int i = 0; i< 3; i++) {
        stringstream trace_name;
        trace_name << vm_id << "." << ss_id_prefix << i << "-full.vhd.bv4";
        ofstream ofs(trace_name.str().c_str(), ios::out | ios::binary | ios::trunc);
        Block blk;
        uint32_t offset = 0;
        while (offset < VM_SIZE) {
            blk.size_ = 16 * 1024;
            blk.offset_ = offset + blk.size_;
            offset += blk.size_;
        }
    }
}










