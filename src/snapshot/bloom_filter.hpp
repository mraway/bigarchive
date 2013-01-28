template<typename ElemType>
template<typename FunctionType>
BloomFilter<ElemType>::BloomFilter(
                                   uint64_t totalElemNum, 
                                   double falsePositiveProbability,
                                   FunctionType* hashFunctions,
                                   int16_t hashFuncNumber)
{
    if (totalElemNum <=0
        || falsePositiveProbability < 0.0
        || falsePositiveProbability > 1.0
        || hashFuncNumber <= 0 )
    {
        THROW_EXCEPTION(BloomFilterParameterInvalid, "argument for bloomfilter constructor must be positive");
    }
    mTotalBits = ((GetM(hashFuncNumber, totalElemNum, falsePositiveProbability) + 7) / 8) * 8;
    if (!mTotalBits)
    {
        THROW_EXCEPTION(BloomFilterParameterInvalid, "argument for bloomfilter constructor error");
    }
    mHashFunctionNum = hashFuncNumber;
    for (int16_t i = 0; i < mHashFunctionNum; i++)
    {
        mHashFunctions.push_back(std::tr1::function<uint64_t (ElemType)>(hashFunctions[i]));
    }
    mBitsSpace.reset(new BitSet(mTotalBits));
}

template<typename ElemType>
BloomFilter<ElemType>::~BloomFilter()
{
}

template<typename ElemType>
void BloomFilter<ElemType>::AddElement(const ElemType& elem)
{
    int i = 0;
    for (; i < mHashFunctionNum; ++i)
    {
        uint64_t position = CalculatePosition(mHashFunctions[i](elem), mTotalBits);
        if (position < 0 || position >= mTotalBits)
        {
            THROW_EXCEPTION(BadHashFunction, "Bloom filter AddElement");
        }
        mBitsSpace->SetPosition(position);
    }
}

template<typename ElemType>
bool BloomFilter<ElemType>::Exist(const ElemType& elem)
{
    int i = 0;
    for (; i < mHashFunctionNum; ++i)
    {
        uint64_t position = CalculatePosition(mHashFunctions[i](elem), mTotalBits);
        if (position < 0 || position >= mTotalBits)
        {
            THROW_EXCEPTION(BadHashFunction, "Bloom filter Exist");
        }
        if (!mBitsSpace->GetPosition(position))
        {
            return false;
        }
    }
    return true;
}

template<typename ElemType>
bool BloomFilter<ElemType>::Exist(const ElemType& elem, std::vector<uint64_t>& hashValues)
{
    int i = 0;
    for (; i < mHashFunctionNum; ++i)
    {
        while (hashValues.size() <= i)
        {
            int idx = hashValues.size();
            hashValues.push_back(mHashFunctions[idx](elem));
        }
        uint64_t position = CalculatePosition(hashValues[i], mTotalBits);
        if (position < 0 || position >= mTotalBits)
        {
            THROW_EXCEPTION(BadHashFunction, "Bloom filter Exist");
        }
        if (!mBitsSpace->GetPosition(position))
        {
            return false;
        }
    }
    return true;
}

template<typename ElemType>
void BloomFilter<ElemType>::Clear()
{
    mBitsSpace->Clear();
}

template<typename ElemType>
template<typename FunctionType>
void BloomFilter<ElemType>::Reset(FunctionType* hashFunctions,
                                  int16_t hashFunctionNum)
{
    if (hashFunctionNum <= 0)
    {
        THROW_EXCEPTION(BloomFilterParameterInvalid, "Bloom filter Reset error");
    }
    Clear();
    mHashFunctionNum = hashFunctionNum;
    for (int16_t i = 0; i < mHashFunctionNum; ++i)
    {
        mHashFunctions.push_back(std::tr1::function<uint64_t (ElemType)>(hashFunctions[i]));
    }
}

