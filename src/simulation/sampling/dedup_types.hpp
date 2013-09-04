#ifndef _DEDUP_TYPES_H_
#define _DEDUP_TYPES_H_

#include <cstdlib>
#include <stdint.h>	// for int types
#include <locale.h>
#include <vector> 
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>

#include <openssl/sha.h>
#include <stdarg.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

using namespace std;

#define CKSUM_LEN 20			// length of sha-1 checksum
#define RECORD_SIZE 36			// size of each block in scan log
#define READ_BUFFER_SIZE 0x800000	// 8MB
#define MAX_RETRY 6			// will retry a few times if open fail

#define IN_PARENT 0x01		// 001
#define IN_CDS 0x02			// 010
#define IN_DIRTY_SEG 0x05	// 101

typedef uint8_t Checksum[CKSUM_LEN];	// checksum type

enum FileSystemType {LOCAL = 0, PANGU = 1, KVFILE = 2};	// file system type

static void pr_msg(const char *fmt, ...)
{
	va_list ap;
	char buf[BUFSIZ], *p2;
	const char *p1;
	char *errstr = strerror(errno);

	buf[0] = '\0';
	p2 = buf + strlen(buf);

	/*
	 * Look for "%m" in the string.
	 * If found, then substitute the errno string.
	 */
	for (p1 = fmt; *p1; p1++) {
		if (*p1 == '%' && *(p1+1) == 'm') {
			(void) strcpy(p2, errstr);
			p2 += strlen(errstr);
			p1++;
		} else {
			*p2++ = *p1;
		}
	}
	if (p2 > buf && *(p2-1) != '\n')
		*p2++ = '\n';
	*p2 = '\0';

	va_start(ap, fmt);
	(void) vfprintf(stdout, buf, ap);
	fflush(stdout);
	va_end(ap);
}

/*
 * store the information of a variable size block data
 */
class Block {
public:
	uint32_t size_;
	uint32_t file_id_;
	uint64_t offset_;
	Checksum cksum_;

	Block() {};

    Block(const Block& blk) {
        SetValue(blk);
    }

	Block(int sz, const Checksum& ck) {
		Set(sz, ck);
	};

	~Block() {};

    void SetValue(const Block& blk) {
        size_ = blk.size_;
        file_id_ = blk.file_id_;
        offset_ = blk.offset_;
		memcpy(cksum_, blk.cksum_, CKSUM_LEN * sizeof(uint8_t));
    }

	void Set(int sz, const Checksum& ck) {
		size_ = sz;
		memcpy(cksum_, ck, CKSUM_LEN * sizeof(uint8_t));
	}

    uint32_t GetSize()
    {
        return size_;
    }

	void Save(ofstream& os) {
		os.write((char *)cksum_, CKSUM_LEN);
		os.write((char *)&file_id_, sizeof(Block::file_id_));
		os.write((char *)&size_, sizeof(Block::size_));
		os.write((char *)&offset_, sizeof(Block::offset_));
	}

	bool Load(ifstream& is) {
		is.read((char *)cksum_, CKSUM_LEN);
		if (is.gcount() != CKSUM_LEN)
			return false;
		is.read((char *)&file_id_, sizeof(Block::file_id_));
		if (is.gcount() != sizeof(Block::file_id_))
			return false;
		is.read((char *)&size_, sizeof(Block::size_));
		if (is.gcount() != sizeof(Block::size_))
			return false;
		is.read((char *)&offset_, sizeof(Block::offset_));
		if (is.gcount() != sizeof(Block::offset_))
			return false;
		return true;
	}

	bool operator==(const Block& other) const {
		return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) == 0;
	}

	bool operator!=(const Block& other) const {
		return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) != 0;
	}

	bool operator<(const Block& other) const {
		return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) < 0;
	}

};

class Segment {
public:
	std::vector<Block> blocklist_;
	//uint32_t min_idx_;	// location of min-hash block in blocklist_
	uint32_t size_;		// overall number of bytes
	//Checksum cksum_;	// segment checksum is the SHA-1 of all block hash values

private:
	//SHA_CTX *ctx_;

public:
	Segment() {
		//min_idx_ = 0;
		size_ = 0;
	}

	void Init() {
		//min_idx_ = 0;
		size_ = 0;
		//ctx_ = new SHA_CTX;
		//SHA1_Init(ctx_);
        blocklist_.clear();
		blocklist_.reserve(256);
	}

	void AddBlock(const Block& blk) {
		blocklist_.push_back(blk);
		size_ += blk.size_;
		//SHA1_Update(ctx_, blk.cksum_, CKSUM_LEN);
		//if (blk.cksum_ < blocklist_[min_idx_].cksum_)
			//min_idx_ = blocklist_.size() - 1;
	}

    void Clear() 
    {
        //min_idx_ = 0;
        size_ = 0;
        blocklist_.clear();
    }

	//void Final() 
    //{
		//SHA1_Final(cksum_, ctx_);
		//delete ctx_;
	//}

	uint64_t GetOffset() 
    {
		if (blocklist_.size() == 0)
			return 0;
		return blocklist_[0].offset_;
	}

    uint32_t GetSize()
    {
        return size_;
    }

	// minhash value can be used this way
	//uint8_t* GetMinHash() 
    //{
		//return blocklist_[min_idx_].cksum_;
	//}

    // put minhash into a string so other stl containers can use it
    //string GetMinHashString() const
    //{
        //string s;
        //s.resize(CKSUM_LEN);
        //s.assign((char *)blocklist_[min_idx_].cksum_, CKSUM_LEN);
        //return s;
    //}

    void SortByHash()
    {
        std::sort(blocklist_.begin(), blocklist_.end());
    }

    bool SearchBlock(const Block& blk)
    {
        return std::binary_search(blocklist_.begin(), blocklist_.end(), blk);
    }

    void Save(ofstream& os) 
    {
        uint32_t num_blocks = blocklist_.size();
        //os.write((char *)&num_blocks, sizeof(uint32_t));
        for (uint32_t i = 0; i < num_blocks; i ++)
            blocklist_[i].Save(os);
    }

	bool Load(ifstream& is) 
    {
		Block blk;
        uint32_t num_blocks;
		Init();
        
        is.read((char *)&num_blocks, sizeof(uint32_t));
        if (is.gcount() != sizeof(uint32_t))
            return false;
        for (uint32_t i = 0; i < num_blocks; i ++) {
            if (blk.Load(is))
                AddBlock(blk);
            else
                return false;
        }

        //Final();
        return true;
    }

	//bool operator==(const Segment& other) const {
		//return memcmp(this->cksum_, other.cksum_, CKSUM_LEN) == 0;
	//}
        bool operator==(const Segment& other) const {
            if (blocklist_.size() != other.blocklist_.size())
                return false;
            for(int i = 0; i < blocklist_.size();i++)
            {
                if (blocklist_[i] != other.blocklist_[i])
                    return false;
            }
            return true;
        }
};

#endif /* _DEDUP_TYPES_H_ */
