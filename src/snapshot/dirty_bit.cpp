#include "dirty_bit.h"
#include "trace_types.h"

DirtyBitMap::DirtyBitMap()
{
}

DirtyBitMap::~DirtyBitMap()
{
}

void DirtyBitMap::Generate(istream& current, istream& parent)
{
    current.seekg(-RECORD_SIZE, ios::end);
    Block blk;
    blk.FromStream(current);
    uint32_t number_segments = (blk.offset_ + blk.size_) / FIX_SEGMENT_SIZE + 1;
    current.seekg(0, ios::beg);
    bitmap_.reserve(number_segments);

    Segment s1, s2;
    int i = 0;
    while (s1.LoadFixSize(current)) {
        if (s2.LoadFixSize(parent)) {
            bitmap_[i] = 1;
            continue;
        }
        if (s1 == s2)
            bitmap_[i] = 0;
        else
            bitmap_[i] = 1;
    }
}

void DirtyBitMap::ToStream(ostream& os)
{
    os.write(bitmap_.data(), bitmap_.size());
}

void DirtyBitMap::FromStream(istream& is)
{
    is.seekg(0, ios::end);
    uint32_t size = is.tellg();
    is.seekg(0, ios::beg);
    bitmap_.resize(size);
    is.read(bitmap_.data(), size);
}

bool DirtyBitMap::Test(size_t pos)
{
    if (pos < bitmap_.size())
        return bitmap_[pos];
    else
        return false;
}










