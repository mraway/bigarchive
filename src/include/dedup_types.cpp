#include "dedup_types.h"
#include <string.h>

Block::Block(int size, const Checksum& ck) 
{
    size_ = size;
    memcpy(cksum_, ck, CKSUM_LEN * sizeof(uint8_t));
}

void Block::ToStream(ostream& os) 
{
    os.write((char *)cksum_, CKSUM_LEN);
    os.write((char *)&file_id_, sizeof(Block::file_id_));
    os.write((char *)&size_, sizeof(Block::size_));
    os.write((char *)&offset_, sizeof(Block::offset_));
}

bool Block::FromStream(istream& is) 
{
    char buf[RECORD_SIZE];
    is.read(buf, RECORD_SIZE);
    if (is.gcount() != RECORD_SIZE)
        return false;
    memcpy(cksum_, buf, CKSUM_LEN);
    memcpy(&file_id_, buf + CKSUM_LEN, sizeof(Block::file_id_));
    memcpy(&size_, buf + CKSUM_LEN + sizeof(Block::file_id_), sizeof(Block::size_));
    memcpy(&offset_, buf + CKSUM_LEN + sizeof(Block::file_id_) + sizeof(Block::size_), sizeof(Block::offset_));
    return true;
}

bool Block::operator==(const Block& other) const 
{
    return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) == 0;
}

bool Block::operator!=(const Block& other) const 
{
    return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) != 0;
}

bool Block::operator<(const Block& other) const 
{
    return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) < 0;
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
    memset(cksum_, '\0', CKSUM_LEN);
    ctx_ = new SHA_CTX;
    SHA1_Init(ctx_);
    blocklist_.clear();
    blocklist_.reserve(256);
}

void Segment::AddBlock(const Block& blk) 
{
    blocklist_.push_back(blk);
    size_ += blk.size_;
    SHA1_Update(ctx_, blk.cksum_, CKSUM_LEN);
    if (blk.cksum_ < blocklist_[min_idx_].cksum_)
        min_idx_ = blocklist_.size() - 1;
}

void Segment::Final() 
{
    SHA1_Final(cksum_, ctx_);
    delete ctx_;
}

uint64_t Segment::GetOffset() 
{
    if (blocklist_.size() == 0)
        return 0;
    return blocklist_[0].offset_;
}

// minhash value can be used this way
uint8_t* Segment::GetMinHash() 
{
    return blocklist_[min_idx_].cksum_;
}

// put minhash into a string so other stl containers can use it
string Segment::GetMinHashString() const
{
    string s;
    s.resize(CKSUM_LEN);
    s.assign((char *)blocklist_[min_idx_].cksum_, CKSUM_LEN);
    return s;
}

void Segment::ToStream(ostream &os)
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
    return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) == 0;
}
