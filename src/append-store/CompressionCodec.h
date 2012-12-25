#ifndef COMPRESSIONCODEC_H_
#define COMPRESSIONCODEC_H_
// #include <stdint.h>
#include <tr1/memory>
#include <string.h>

class LzoCompressor;
class LzoDecompressor;

class CompressionCodec 
{
public:
    virtual ~CompressionCodec();

    /*
    *  it is the caller's responsibility to allocate enough memory for outputData.
    *  success if return value is 0;
    *  error if return value < 0;
    */
    virtual int compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize) = 0;

    /*
    *  success if return value is 0 
    *  error if return value < 0 
    */
    virtual int decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize) = 0;

    // how much buffer is needed for the output data (from origianl to compressed)
    virtual uint32_t getBufferSize(uint32_t) = 0;

    static CompressionCodec* getCodec(const char* name, int, bool writeflag=true);
};

typedef std::tr1::weak_ptr<CompressionCodec> CompressionCodecWeakPtr;
typedef std::tr1::shared_ptr<CompressionCodec> CompressionCodecPtr;


class LzoCodec: public CompressionCodec 
{
public:
    LzoCodec(int, bool); 

    virtual ~LzoCodec(); 

    int compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize);

    int decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize);

    uint32_t getBufferSize(uint32_t);

    static const char* mName;

private:
    std::tr1::shared_ptr<LzoCompressor>   mComPtr;
    std::tr1::shared_ptr<LzoDecompressor> mDecomPtr;

    uint32_t mBufsize;
    bool     mIsWriter;
};


class NoneCodec: public CompressionCodec 
{
public:
    NoneCodec(int);

    virtual ~NoneCodec();

    int compress(char* data, uint32_t datasize, char* outputData, uint32_t& compressedsize);

    int decompress(char* data, uint32_t datasize, char* outputData, uint32_t& uncompressedsize);

    uint32_t getBufferSize(uint32_t);

    static const char* mName;
};


#endif /* COMPRESSIONCODEC_H_ */
