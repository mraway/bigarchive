#include <stdio.h>
#include "lzo/lzoconf.h"
#include "lzo/lzo1x.h"
#include "Compressor.h"

LzoCompressor::LzoCompressor(int bufsize) 
{
    mBufsize = 0;

    // TODO: add support for other LZO algorithms, only LZO1X_1 for now
    mCharVec.resize(LZO1X_1_MEM_COMPRESS);
}

uint32_t LzoCompressor::getOutBufferSize(uint32_t value) 
{
    // compressed data may be longer, the formula is based on LZO docs.
    mMaxOutSize = value + value / 16 + 64 + 3;
    return mMaxOutSize;
}

LzoCompressor::~LzoCompressor() 
{
}

int LzoCompressor::compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize)
{
    lzo_uint outlen = 0;
    int retcode = lzo1x_1_compress((const unsigned char*)data, datasize, 
                           (unsigned char*)outputData, &outlen, (unsigned char*)&mCharVec[0]);
    if (retcode == LZO_E_OK) 
    {
        compressedsize = outlen;
        return 0;
    }
    else {
        return retcode; 
    }
}


LzoDecompressor::LzoDecompressor(int blocksize) 
{
}

LzoDecompressor::~LzoDecompressor() 
{
}

uint32_t LzoDecompressor::getOutputBufferSize(uint32_t value) 
{
    mBufsize = value + value / 16 + 64 + 3;
    return mBufsize;
}

int LzoDecompressor::decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize)
{
    lzo_uint outlen = 0;
    int retcode = lzo1x_decompress((const unsigned char*)data, datasize, 
                                   (unsigned char*)outputData, &outlen, NULL);
    if (retcode == LZO_E_OK) 
    {
        uncompressedsize = outlen;
        return 0;
    }
    else {
        return retcode;
    }
}

