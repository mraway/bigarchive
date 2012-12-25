#ifndef COMPRESSOR_H_
#define COMPRESSOR_H_

#include <vector>
#include <stdint.h>

class Compressor 
{
public:
    virtual ~Compressor() { }

    /* caller need to make sure there is enough output buffer
    */
    virtual int compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize) = 0;

    // get the output buffer size
    virtual uint32_t getOutBufferSize(uint32_t) = 0;
};

class LzoCompressor : public Compressor 
{
public:
    LzoCompressor(int bufsize = 64 * 1024);

    virtual ~LzoCompressor();

    int compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize);

    uint32_t getOutBufferSize(uint32_t);

private:
    uint32_t mBufsize;
    uint32_t mMaxOutSize;

    std::vector<unsigned char> mCharVec;
};


class Decompressor 
{
public:
    virtual ~Decompressor() { }

    /* caller need to make sure there is enough output buffer
    */
    virtual int decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize) = 0;

    virtual uint32_t getOutputBufferSize(uint32_t) = 0;
};

class LzoDecompressor : public Decompressor 
{
public:
    LzoDecompressor(int);

    virtual ~LzoDecompressor();

    int decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize);

    uint32_t getOutputBufferSize(uint32_t);

private:
    uint32_t mBufsize;
};

#endif /* COMPRESSOR_H_ */
