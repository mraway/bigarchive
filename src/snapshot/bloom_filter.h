#ifndef _BLOOMFILTER_H_
#define _BLOOMFILTER_H_

#include <cstring>
#include <cmath>
#include <stdexcept>
#include <vector>
#include <tr1/functional>
#include <tr1/memory>
#include "exception.h"
//#include "dynamic_buffer.h"
#include <iostream>

class BloomFilterExceptionBase : public ExceptionBase
{
public:
    DEFINE_EXCEPTION(BloomFilterExceptionBase, ExceptionBase);
};

class BadHashFunction : public BloomFilterExceptionBase
{
public:
    DEFINE_EXCEPTION(BadHashFunction, BloomFilterExceptionBase);
};

class BloomFilterParameterInvalid : public BloomFilterExceptionBase
{
public:
    DEFINE_EXCEPTION(BloomFilterParameterInvalid, BloomFilterExceptionBase);
};

class BitSet
{
public:
    BitSet(uint64_t totalBits)
    {
        mBitsSpace = new char[(totalBits + 7) / 8];
        mTotalBits = totalBits;
        mMaxTotalBits = mTotalBits;
        memset(mBitsSpace, 0, (mTotalBits + 7) / 8);
        mOwns = true;
    }
        
    ~BitSet()
    {
        if (mOwns)
        {
            delete[] mBitsSpace;
        }
    }

    inline void SetPosition(uint64_t position)
    {
        if (position < 0 || position >= mTotalBits)
        {
            THROW_EXCEPTION(BloomFilterParameterInvalid, "in BitSet");
        }
        uint64_t byteShift = position / 8;
        uint64_t bitShift = position % 8;
        mBitsSpace[byteShift] |= (0x80 >> bitShift);
    }
	
    inline char GetPosition(uint64_t position)
    {
        if (position < 0 || position >= mTotalBits)
        {
            THROW_EXCEPTION(BloomFilterParameterInvalid, "in BitGet");
        }
        uint64_t byteShift = position / 8;
        uint64_t bitShift = position % 8;
        return mBitsSpace[byteShift] & (0x80 >> (bitShift));
    }

    inline void Clear()
    {
        memset(mBitsSpace, 0, (mTotalBits + 7) / 8);
    }
        
    inline void Resize(uint64_t newSize)
    {
        if (newSize <= 0)
        {
            THROW_EXCEPTION(BloomFilterParameterInvalid, "parameter size is negative");
        }
        if (((newSize + 7) / 8) <= ((mMaxTotalBits + 7) / 8))
        {
            memset(mBitsSpace, 0, (newSize + 7) / 8);
        }
        else
        {
            delete[] mBitsSpace;
            mBitsSpace = new char[(newSize + 7) / 8];
            memset(mBitsSpace, 0, (newSize + 7) / 8);
            mMaxTotalBits = ((newSize + 7) / 8) * 8;
        }
        mTotalBits = newSize;
    }
    /*
    inline void Serialization(DynamicBuffer& buffer)
    {
        uint64_t lenTemp = static_cast<uint64_t>((mTotalBits + 7) / 8);
        uint64_t totalLength = sizeof(uint64_t) + lenTemp;
        buffer.Reserve(totalLength);
        buffer.Add(reinterpret_cast<char*>(&mTotalBits), sizeof(uint64_t));
        buffer.Add(mBitsSpace, lenTemp);
    }
       
    inline bool DeSerialization(DynamicBuffer& buffer)
    {
        uint64_t len = static_cast<uint64_t>(buffer.DataLength());
        if (len == 0)
        {
            return false;
        }
        char* curr = const_cast<char*>(buffer.Buffer());
        mTotalBits = *reinterpret_cast<uint64_t*>(curr);
        curr += sizeof(uint64_t);
        mMaxTotalBits = mTotalBits;
        if (mBitsSpace && mOwns)
        {
            delete[] mBitsSpace;
        }
        mBitsSpace = new char[(mTotalBits + 7) / 8];
        memcpy(mBitsSpace, curr, ((mTotalBits + 7) / 8));
        return true;
    }
    */  
    inline void Serialization(ostream& os)
    {
        uint64_t lenTemp = static_cast<uint64_t>((mTotalBits + 7) / 8);
        os.write(reinterpret_cast<char*>(&mTotalBits), sizeof(uint64_t));
        os.write(mBitsSpace, lenTemp);
    }        

    inline bool DeSerialization(istream& is)
    {
        is.read(reinterpret_cast<char*>(&mTotalBits), sizeof(uint64_t));
        if (is.gcount() != sizeof(uint64_t))
            return false;
        mMaxTotalBits = mTotalBits;
        if (mBitsSpace && mOwns)
        {
            delete[] mBitsSpace;
        }
        uint64_t lenTemp = static_cast<uint64_t>((mTotalBits + 7) / 8);
        mBitsSpace = new char[lenTemp];
        is.read(mBitsSpace, lenTemp);
        if (static_cast<uint64_t>(is.gcount()) != lenTemp)
            return false;
        return true;
    }

    uint64_t GetCurrentTotalBits()
    {
        return mTotalBits;
    }
private:
    char* mBitsSpace;
    uint64_t mTotalBits;
    uint64_t mMaxTotalBits;
    bool mOwns;
};

/**
 * BloomFilter is a space-efficient probabilistic data structure that is
 * used to test whether an element is a member of a set. False positives
 * are possible, but false negatives are not. Elements can be added to the
 * set, but not removed(though this can be addressed with a counting filter).
 * The more elements that are added to the set, the larger the probability of
 * false positives. want more details, just see wikipedia 
 * http://en.wikipedia.org/wiki/Bloom_filter
 */

/**
 * false positive probability can be modeled as:
 * P = (1 - exp(-kn/m))^k, where k represents hash function number
 * m represents total bit number, n represents total element number
 * following function will calculte the parameter if given
 * other parameters. you must use these functions before create your
 * own BloomFilter to decide input parameters. These function do not
 * check result overflow.
 */

inline uint64_t GetN(int16_t k, int64_t m, double p)
{
    if (k <= 0 || m <= 0 || p > 1.0 || p < 0.0)
    {
        THROW_EXCEPTION(BloomFilterParameterInvalid, "BloomFilter GetN");
    }
    double midResult = (log(p) / k);
    midResult = -1 * m * log(1- pow(M_E, midResult)) / k;
    return static_cast<uint64_t>(midResult + 1);
}
/**
 * Get the K which minimize p
 */
inline int16_t GetK(int64_t n, int64_t m)
{
    if (n <= 0 || m <= 0)
    {
        THROW_EXCEPTION(BloomFilterParameterInvalid, "BloomFilter GetK");
    }
    return static_cast<int16_t>(0.7 * m / n);
}

inline uint64_t GetM(int16_t k, uint64_t n, double p, uint32_t maxM = 0)
{
    if (k <= 0 || n <=0 || p < 0 || p > 1.0)
    {
        THROW_EXCEPTION(BloomFilterParameterInvalid, "BloomFilter GetM");
    }
    double midResult = log(1- pow(M_E,(log(p) / k)));
    uint64_t m = static_cast<uint64_t>(-1 / midResult * k * n);
    if (maxM != 0 && m > maxM)
    {
	    m = maxM;
    }
    return m;
}

template<typename ElemType>
class BloomFilter
{
public:
    /**
     * must give number of total elements and desired false positive probability
     * to let bloomfilter calculate the corresponding bit spcace. FuctionType must
     * be uint64_t (ElemType element). 
     * @param totalElemNum element number expected in set
     * @param falsePositiveProbability false positive probability you want
     * @param hashFunctions hash function pointer array
     * @param hashFuncNumber the number of hash functions
     */
    template <typename FunctionType>
    BloomFilter(uint64_t totalElemNum, 
                double falsePositiveProbability,
                FunctionType* hashFunctions,
                int16_t hashFuncNumber);
        
    ~BloomFilter();
        
    void AddElement(const ElemType& key);
        
    /**
     * query whether given element does exist
     * @param element given element
     * @return true if exist. false otherwise
     */
        
    bool Exist(const ElemType& element);
        
    /**
     * query whether given element does exist
     * @param element given element
     * @param hashValues hash values of the given element
     * @return true if exist. false otherwise
     */
        
    bool Exist(const ElemType& element, std::vector<uint64_t>& hashValues);
        
    /**
     * clear set when elementnum is large. Clear and reinsert
     * is always to make the probability of false positives smaller,
     * .i.e to reduce conflict probability between elements. Consider
     * the senario that elements are frequently removed
     */
        
    void Clear();
        
    /**
     * reset hash functions, FunctionType must be 
     * uint64_t(ElemType element)
     * @param hashFunctions new hash functions set up
     * @param hashFuncNumber number of hash functions
     */
    template<typename FunctionType>
    void Reset(FunctionType* hashFunctions, int16_t hashFuncNumber);
        
    /**
     * Serialization to Dynamicbuffer
     */
    /*
    void Serialization(DynamicBuffer& buffer)
    {
        mBitsSpace->Serialization(buffer);
    }
    */
    /**
     * DeSerialization from  Dynamicbuffer
     */
    /*
    bool DeSerialization(DynamicBuffer& buffer)
    {
        if (mBitsSpace->DeSerialization(buffer)) {
            mTotalBits = mBitsSpace->GetCurrentTotalBits();
            return true;
        }
        return false;
    }
    */
    /*
     * Serialize to stream
     */
    void Serialization(ostream& os) {
        mBitsSpace->Serialization(os);
    }

    bool DeSerialization(istream& is) {
        if (mBitsSpace->DeSerialization(is)) {
            mTotalBits = mBitsSpace->GetCurrentTotalBits();
            return true;
        }
        return false;
    }

    uint64_t GetCurrentTotalBits()
    {
        return mTotalBits;
    }

protected:
    
    /**
     * Calculate the position in BitSet 
     */
    virtual uint64_t CalculatePosition(uint64_t hashValue, uint64_t totalBits) const
    {
        return hashValue % totalBits;
    }

private:
    std::tr1::shared_ptr<BitSet> mBitsSpace;
    uint64_t mTotalBits;
    std::vector<std::tr1::function<uint64_t (ElemType)> > mHashFunctions;
    int16_t mHashFunctionNum;
};

#include "bloom_filter.hpp"
#endif
