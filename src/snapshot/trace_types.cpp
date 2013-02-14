#include "trace_types.h"
#include <string.h>
#include <sstream>

Checksum::Checksum()
{
    memset(data_, '\0', CKSUM_LEN);
}

Checksum& Checksum::operator=(const Checksum& other)
{
    if (this == &other)
        return *this;
    memcpy(data_, other.data_, CKSUM_LEN);
    return *this;
}

bool Checksum::operator==(const Checksum& other) const
{
    return memcmp(data_, other.data_, CKSUM_LEN) == 0;
}

bool Checksum::operator!=(Checksum const &other) const
{
    return memcmp(data_, other.data_, CKSUM_LEN) != 0;
}

bool Checksum::operator<(const Checksum& other) const
{
    return memcmp(data_, other.data_, CKSUM_LEN) < 0;
}

void Checksum::ToStream(ostream& os) const
{
    os.write((char*)data_, CKSUM_LEN);
}

bool Checksum::FromStream(istream& is)
{
    is.read((char*)data_, CKSUM_LEN);
    if (is.gcount() != CKSUM_LEN)
        return false;
    return true;
}

string Checksum::ToString()
{
    stringstream ss;
    unsigned char hexchar[16];
    for (unsigned char i = 0; i < 16; ++i)
        hexchar[i] = '0' + i;
    for (int i = 0; i < CKSUM_LEN; ++i)
    {
        ss << hexchar[data_[i] >> 4];
        ss << hexchar[data_[i] & 0xF];
    }
    return ss.str();
}

uint32_t Checksum::First4Bytes() const
{
    uint32_t tmp;
    memcpy((char*)&tmp, data_, 4);
    return tmp;
}

uint32_t Checksum::Last4Bytes() const
{
    uint32_t tmp;
    memcpy((char*)&tmp, &data_[CKSUM_LEN - 4], 4);
    return tmp;
}

Block::Block(int size, const Checksum& ck) 
{
    size_ = size;
    cksum_ = ck;
}

void Block::ToStream(ostream& os) const
{
    cksum_.ToStream(os);
    os.write((char *)&file_id_, sizeof(Block::file_id_));
    os.write((char *)&size_, sizeof(Block::size_));
    os.write((char *)&offset_, sizeof(Block::offset_));
}

bool Block::FromStream(istream& is) 
{
    if (!cksum_.FromStream(is))
        return false;

    int nbytes = RECORD_SIZE - CKSUM_LEN;
    char buf[RECORD_SIZE - CKSUM_LEN];
    is.read(buf, nbytes);
    if (is.gcount() != nbytes)
        return false;
    memcpy(&file_id_, buf, sizeof(Block::file_id_));
    memcpy(&size_, buf + sizeof(Block::file_id_), sizeof(Block::size_));
    memcpy(&offset_, buf + sizeof(Block::file_id_) + sizeof(Block::size_), sizeof(Block::offset_));
    return true;
}

string Block::ToString()
{
    stringstream ss;
    ss << "checksum: " << cksum_.ToString();    
    ss << " offset: " << offset_;
    ss << " size: " << size_;
    ss << " file_id: " << file_id_;
    return ss.str();
}

bool Block::operator==(const Block& other) const 
{
    return cksum_ == other.cksum_;
}

bool Block::operator!=(const Block& other) const 
{
    return cksum_ != other.cksum_;
}

bool Block::operator<(const Block& other) const 
{
    return cksum_ < other.cksum_;
}

Segment::Segment() 
{
    min_idx_ = 0;
    size_ = 0;
}

void Segment::Init() 
{
    min_idx_ = 0;
    size_ = 0;
    ctx_ = new SHA_CTX;
    SHA1_Init(ctx_);
    blocklist_.clear();
    blocklist_.reserve(256);
}

void Segment::AddBlock(const Block& blk) 
{
    blocklist_.push_back(blk);
    size_ += blk.size_;
    SHA1_Update(ctx_, blk.cksum_.data_, CKSUM_LEN);
    if (blk.cksum_ < blocklist_[min_idx_].cksum_)
        min_idx_ = blocklist_.size() - 1;
}

void Segment::Final() 
{
    SHA1_Final((unsigned char*)cksum_.data_, ctx_);
    delete ctx_;
}

uint64_t Segment::GetOffset() 
{
    if (blocklist_.size() == 0)
        return 0;
    return blocklist_[0].offset_;
}

// minhash value can be used this way
Checksum& Segment::GetMinHash() 
{
    return blocklist_[min_idx_].cksum_;
}

void Segment::ToStream(ostream &os) const
{
    uint32_t num_blocks = blocklist_.size();
    os.write((char *)&num_blocks, sizeof(uint32_t));
    for (uint32_t i = 0; i < num_blocks; i ++)
        blocklist_[i].ToStream(os);
}

bool Segment::FromStream(istream &is)
{
    Block blk;
    uint32_t num_blocks;
    Init();
        
    is.read((char *)&num_blocks, sizeof(uint32_t));
    if (is.gcount() != sizeof(uint32_t))
        return false;
    for (uint32_t i = 0; i < num_blocks; i ++) {
        if (blk.FromStream(is))
            AddBlock(blk);
        else
            return false;
    }

    Final();
    return true;
}

bool Segment::LoadFixSize(istream &is)
{
    Block blk;
    Init();
    while (blk.FromStream(is)) {
        if (blk.size_ == 0) {       // fix the zero-sized block bug in scanner
            continue;
        }
        AddBlock(blk);
        if (size_ >= FIX_SEGMENT_SIZE)
            break;
    }
    Final();
    if (size_ == 0)
        return false;
    else
        return true;
}

bool Segment::operator==(const Segment& other) const 
{
    return cksum_ == other.cksum_;
}
