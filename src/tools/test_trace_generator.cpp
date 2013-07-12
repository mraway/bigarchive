#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <string.h>
#include "../snapshot/trace_types.h"

using namespace std;

int main(int argc, char** argv)
{
    vector<Block> trace;
    Block blk;
    uint32_t offset;
    uint64_t j;
    ofstream ofs;
    string fname;

    trace.clear();
    fname = "vm-test.1-1-1-full.vhd.bv4";
    offset = 0;
    for (uint64_t i = 0; i < 128 * 5; i++) {
        j = i;
        blk.size_ = 16 * 1024;
        blk.offset_ = offset;
        offset += blk.size_;
        memset(&blk.cksum_.data_[0], '\0', CKSUM_LEN);
        memcpy(&blk.cksum_.data_[0], (char*)&j, sizeof(uint64_t));

        cout << blk.ToString() << " " << j << endl;
        if (trace.size() >= 1 && blk == trace.back()) {
            cout << "yes" << endl;
        }
        else {
            cout << "no" << endl;
        }

        trace.push_back(blk);
    }
    ofs.open(fname.c_str(), ios::out | ios::binary | ios::trunc);
    for (vector<Block>::iterator it = trace.begin(); it != trace.end(); ++ it) {
        it->ToStream(ofs);
    }
    ofs.close();

    trace.clear();
    fname = "vm-test.1-1-2-full.vhd.bv4";
    offset = 0;
    for (uint64_t i = 0; i < 128 * 5; i++) {
        if (i >= 128) {
            j = i + 64;
        }
        else {
            j = i;
        }
        blk.size_ = 16 * 1024;
        blk.offset_ = offset;
        offset += blk.size_;
        memset(&blk.cksum_.data_[0], '\0', CKSUM_LEN);
        memcpy(&blk.cksum_.data_[0], &j, sizeof(uint64_t));
        trace.push_back(blk);
    }
    ofs.open(fname.c_str(), ios::out | ios::binary | ios::trunc);
    for (vector<Block>::iterator it = trace.begin(); it != trace.end(); ++ it) {
        it->ToStream(ofs);
    }
    ofs.close();

    trace.clear();
    fname = "vm-test.1-1-3-full.vhd.bv4";
    offset = 0;
    for (uint64_t i = 0; i < 128 * 5; i++) {
        if (i >= 128) {
            j = i + 128;
        }
        else {
            j = i;
        }
        blk.size_ = 16 * 1024;
        blk.offset_ = offset;
        offset += blk.size_;
        memset(&blk.cksum_.data_[0], '\0', CKSUM_LEN);
        memcpy(&blk.cksum_.data_[0], &j, sizeof(uint64_t));
        trace.push_back(blk);
    }
    ofs.open(fname.c_str(), ios::out | ios::binary | ios::trunc);
    for (vector<Block>::iterator it = trace.begin(); it != trace.end(); ++ it) {
        it->ToStream(ofs);
    }
    ofs.close();

    trace.clear();
    fname = "vm-test.1-1-4-full.vhd.bv4";
    offset = 0;
    for (uint64_t i = 0; i < 128 * 5; i++) {
        if (i >= 128) {
            j = i + 192;
        }
        else {
            j = i;
        }
        blk.size_ = 16 * 1024;
        blk.offset_ = offset;
        offset += blk.size_;
        memset(&blk.cksum_.data_[0], '\0', CKSUM_LEN);
        memcpy(&blk.cksum_.data_[0], &j, sizeof(uint64_t));
        trace.push_back(blk);
    }
    ofs.open(fname.c_str(), ios::out | ios::binary | ios::trunc);
    for (vector<Block>::iterator it = trace.begin(); it != trace.end(); ++ it) {
        it->ToStream(ofs);
    }
    ofs.close();
}



















