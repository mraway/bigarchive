#ifndef SERIALIZE_H
#define SERIALIZE_H

#include <iostream>
#include <map>
#include <vector>
#include <string>
#include "exception.h"
#include <stdint.h>

namespace marshall
{

/**
 * Base interface class for all serializable objects including cangjie message and streamline records
 */
class Serializable
{
public:
    /**
     * Destructor
     */
    virtual ~Serializable(){}
    /**
     * Serialize to a stream.
     */
    virtual void Serialize(std::ostream& os) const = 0;
    /**
     * De-serialize from a stream.
     */
    virtual void Deserialize(std::istream& is) = 0;

    virtual Serializable* New() = 0;

    /**
     */
    virtual void Copy(const Serializable& from) = 0;

    virtual Serializable* Clone()
    {
        Serializable* ptr = New();
        ptr->Copy(*this);
        return ptr;
    }

    /**
     */
    virtual void Move(Serializable& from)
    {
        Copy(from);
    }
    /** Return how many bits the this object use.
    */
    virtual int64_t GetSize()
    {
        return int64_t(-1);
    }

    virtual void Compress()
     {}
};

// TODO: add serialization methods for STL containers
// template<typename T>
//         void Serialize(const std::vector<T>& v, std::ostream& os);
template<typename T>
void Serialize(const T& t, std::ostream& os)
{
    t.Serialize(os);
}

template<typename T>
void Deserialize(T& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        t.Deserialize(is);
    }
}

template<>
inline void Serialize<bool>(const bool& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(bool));
}

template<>
inline void Deserialize<bool>(bool& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(bool));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<int64_t>(const int64_t& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(int64_t));
}

template<>
inline void Serialize<uint64_t>(const uint64_t& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(uint64_t));
}

template<>
inline void Deserialize<int64_t>(int64_t& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(int64_t));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Deserialize<uint64_t>(uint64_t& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(uint64_t));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<int8_t>(const int8_t& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(int8_t));
}

template<>
inline void Deserialize<int8_t>(int8_t& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(int8_t));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<uint16_t>(const uint16_t& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(uint16_t));
}

template<>
inline void Deserialize<uint16_t>(uint16_t& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(uint16_t));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<int32_t>(const int32_t& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(int32_t));
}

template<>
inline void Deserialize<int32_t>(int32_t& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(int32_t));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<uint32_t>(const uint32_t& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(uint32_t));
}

template<>
inline void Deserialize<uint32_t>(uint32_t& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(uint32_t));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<double>(const double& t, std::ostream& os)
{
    os.write((char*)&t, sizeof(double));
}

template<>
inline void Deserialize<double>(double& t, std::istream& is)
{
    if (is.eof())
        throw UnexpectedEndOfStreamException("");
    else
    {
        is.read((char*)&t, sizeof(double));
        if (is.bad())
            throw StreamCorruptedException("");
    }
}

template<>
inline void Serialize<std::string>(const std::string& str, std::ostream& os)
{
    uint32_t s = str.size();
    Serialize(s, os);
    os.write(&str[0], s);
}

template<>
inline void Deserialize<std::string>(std::string& str, std::istream& is)
{
    uint32_t s;
    Deserialize(s, is);
    char* buf = new char[s];
    is.read(buf, s);
    if (is.bad())
        throw StreamCorruptedException("");
    str.assign(buf, s);
    delete[] buf;
}

template<typename KeyType, typename ValueType>
void Serialize(const std::map<KeyType, ValueType>& t, std::ostream& os)
{
    SerializeMap(t, os);
}

template<typename KeyType, typename ValueType>
void Deserialize(std::map<KeyType, ValueType>& t, std::istream& is)
{
    DeserializeMap(t, is);
}

template<typename T>
void SerializeMap(const T& t, std::ostream& os)
{
    Serialize((int32_t)t.size(), os);
    typename T::const_iterator i;
    for (i = t.begin();
         i != t.end();
         i++)
    {
        /**
         * Assume there's a
         * template<typename T> unit32_t Serialize(const T&, aostream os)
         * for KeyType and ValueType
         * return: actual bytes written
         */
        Serialize(i->first, os);
        Serialize(i->second, os);
    }
}

template<typename T>
void DeserializeMap(T& t, std::istream& is)
{
    /**
     * Assume there's a
     * template<typename T> void Deserialize(const T&, aistream is)
     * return: actual bytes read, 0 means end of stream
     */
    if(is.eof())
        throw UnexpectedEndOfStreamException("");
    int32_t size;
    Deserialize(size, is);
    typename T::key_type key;
    typename T::mapped_type value;
    while (size > 0)
    {
        if(is.eof())
            throw UnexpectedEndOfStreamException("");
        Deserialize(key, is);
        if(is.eof())
            throw UnexpectedEndOfStreamException("");
        Deserialize(value, is);
        t[key] = value;
        size--;
    }
}



template<typename T>
void Serialize(const std::vector<T>& v, std::ostream& os)
{
    Serialize((uint32_t)v.size(), os);
    for (uint32_t i = 0; i < v.size(); i++)
        Serialize(v[i], os);
}

template<typename T>
void Deserialize(std::vector<T>& v, std::istream& is)
{
    uint32_t s;
    Deserialize(s, is);
    v.resize(s);
    for (uint32_t i = 0; i < s; i++)
        Deserialize(v[i], is);
}

}

#endif
