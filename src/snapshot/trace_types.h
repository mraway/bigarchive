#ifndef _TRACE_TYPES_H_
#define _TRACE_TYPES_H_

#include <cstdlib>
#include <stdint.h>	// for int types
#include <vector> 
#include <iostream>
#include <openssl/sha.h>

using namespace std;

#define CKSUM_LEN 20			// length of sha-1 checksum
#define RECORD_SIZE 36			// size of each block in scan log
#define FIX_SEGMENT_SIZE (2 * 1024 * 1024)	// size of a segment
#define AVG_BLOCK_SIZE (4 * 1024)
#define MAX_BLOCK_SIZE (16 * 1024)

//typedef uint8_t Checksum[CKSUM_LEN];	// checksum type

struct Checksum
{
    char data_[CKSUM_LEN];

    Checksum();

    Checksum& operator=(const Checksum& other);

    bool operator==(const Checksum& other) const;

    bool operator!=(const Checksum& other) const;

    bool operator<(const Checksum& other) const;

    void ToStream(ostream& os) const;

    bool FromStream(istream& is);

    string ToString();

    uint32_t First4Bytes() const;

    uint32_t Last4Bytes() const;
};

/*
 * store the information of a variable size block data
 */
class Block {
public:
	uint32_t size_;
	uint32_t file_id_;
	uint64_t offset_;
	Checksum cksum_;

public:
	Block() {};

	Block(int size, const Checksum& ck);

	~Block() {};

	void ToStream(ostream& os) const;

	bool FromStream(istream& is);

    string ToString();

	bool operator==(const Block& other) const;

	bool operator!=(const Block& other) const;

	bool operator<(const Block& other) const;
};

class Segment {
public:
	std::vector<Block> blocklist_;
	uint32_t min_idx_;	// location of min-hash block in blocklist_
	uint32_t size_;		// overall number of bytes
	Checksum cksum_;	// segment checksum is the SHA-1 of all block hash values

public:
	Segment();

	void Init();

	void AddBlock(const Block& blk);

	void Final();

    /*
     * return the offset of the first block
     */
	uint64_t GetOffset();

	/*
     * minhash value can be used this way
     */
	Checksum& GetMinHash();

    /*
     * Serialization of segment data
     * this is different from a scan trace
     */
    void ToStream(ostream& os) const;
	bool FromStream(istream& is);

    /*
     * Load a fixed segment from scan trace
     */
    bool LoadFixSize(istream& is);

	bool operator==(const Segment& other) const;

private:
	SHA_CTX *ctx_;
};

#endif /* _TRACE_TYPES_H_ */
