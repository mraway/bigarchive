#ifndef _DYNAMIC_BUFFER_H_
#define _DYNAMIC_BUFFER_H_

#include <tr1/inttypes.h>
#include <cstring>

/**
 * DynamicBuffer is designed for small dynamic memory management, an alternative of vector<char>
 * DON'T USE IT FOR LARGE PIECE OF MEMORY MANAGEMENT.
 * NOTE: it's not intended for a memory allocator, but rather a short life-cycle buffer 
 */
class DynamicBuffer
{
public:
    DynamicBuffer(uint32_t initSize = 0, bool owns = true):mSize(initSize), mOwn(owns)
    {
        if (initSize > 0)
            mBuffer = mPtr = new char [initSize];
        else
            mBuffer = mPtr = NULL;
    }

    /**
     * manage memory buf with DynamicBuffer
     */
    DynamicBuffer(char* buf, uint32_t len, bool owns = false):mBuffer(buf), mPtr(buf), mSize(len), mOwn(owns)
    {
    }

    ~DynamicBuffer()
    {
        Release();
    }

    const char* Buffer() const { return mBuffer; }

    uint32_t DataLength() const { return (uint32_t)(mPtr - mBuffer);}
    uint32_t TotalLength() const { return mSize;}

    uint32_t Remaining() const { return (uint32_t)(mSize - (mPtr - mBuffer));};

    /**
     * Ensure we have length memory available. Alloc length*3/2 for future use
     * @param length memory length needed
     */
    void Ensure(uint32_t length)
    {
        if (Remaining() < length)
            Grow((DataLength() + length)*3/2);
    }

    void Reserve(uint32_t length)
    {
        if (Remaining() < length)
            Grow(DataLength() + length);
    }

    /**
     * Add a piece of memory to buffer, length must be specified
     * @param str  memory start pointer
     * @length length memory length to add
     * @return pointer of memory start position
     */
    char* Add(const char* str, uint32_t length)
    {
        if (str == NULL || length == 0) 
            return NULL;

        Ensure(length);
        memcpy(mPtr, str, length);
        char* rPtr = mPtr;
        mPtr += length;
        return rPtr;
    }

    /**
     * Preserve a length memory for caller.
     * @param length memory need to alloc
     * @return  pointer of memory start position
     */
    char* Alloc(uint32_t length)
    {
        if (length == 0)
            return NULL;
        Ensure(length);
        char* rPtr = mPtr;
        mPtr += length;
        return rPtr;
    }

    void Reset(){ mPtr = mBuffer; }

    void Release()
    {
        if (mOwn)
            delete [] mBuffer;
        mBuffer = mPtr = NULL;
        mSize = 0;
    }


protected:

    void Grow(uint32_t newSize)
    {
        char* newBuf = new char [newSize];

        if (mBuffer && (mPtr - mBuffer) > 0)
            memcpy(newBuf, mBuffer, mPtr - mBuffer);

        mPtr = newBuf + (mPtr - mBuffer);
        if (mOwn && mBuffer)
            delete [] mBuffer;
        mBuffer = newBuf;
        mSize = newSize;
        mOwn  = true;
    }

    uint32_t mSize;
    bool   mOwn;
public:
    char* mBuffer;
    char* mPtr; /*< current pointer pos */
};

#endif

