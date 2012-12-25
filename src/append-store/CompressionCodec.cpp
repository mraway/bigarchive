#include <string>
#include "Compressor.h"
#include "CompressionCodec.h"

const char* LzoCodec::mName = "lzo";
const char* NoneCodec::mName = "none";


CompressionCodec::~CompressionCodec() 
{
}

CompressionCodec* CompressionCodec::getCodec(const char* name, int bufsize, bool writeflag) 
{
    CompressionCodec* ret = NULL;

    if (strcmp(name, LzoCodec::mName) == 0) 
    {
        ret = new LzoCodec(bufsize, writeflag);
    } 
    else if (strcmp(name, NoneCodec::mName) == 0) 
    { 
        ret = new NoneCodec(bufsize);
    }
    return ret;
}

LzoCodec::LzoCodec(int bufsize, bool writeflag) 
    : mBufsize(bufsize), mIsWriter(writeflag) 
{
    mDecomPtr.reset(new LzoDecompressor(bufsize));
    if (mIsWriter) 
    {
        mComPtr.reset(new LzoCompressor(bufsize));
    }
}

LzoCodec::~LzoCodec() 
{
}

int LzoCodec::compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize)
{
    if (mIsWriter) 
    {
        return mComPtr->compress(data, datasize, outputData, compressedsize);
    }
    else 
    {
        return -100;
    }
}

int LzoCodec::decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize)
{
    return mDecomPtr->decompress(data, datasize, outputData, uncompressedsize);
}

uint32_t LzoCodec::getBufferSize(uint32_t datasize) 
{
    if (mIsWriter) {
        return mComPtr->getOutBufferSize(datasize);
    }
    return 0;
}


NoneCodec::NoneCodec(int datasize) 
{ 
}

NoneCodec::~NoneCodec() 
{ 
}

uint32_t NoneCodec::getBufferSize(uint32_t bufsize) 
{
    return bufsize + 1;
}

int NoneCodec::compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize)
{
    memmove(outputData, data, datasize);
    compressedsize = datasize;
    return 0;
}

int NoneCodec::decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize)
{
    memmove(outputData, data, datasize);
    uncompressedsize = datasize;
    return 0;
}
