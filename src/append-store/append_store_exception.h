#ifndef APPENDSTORE_EXCEPTION_H
#define APPENDSTORE_EXCEPTION_H

#include "apsara/common/exception.h"

namespace apsara
{
namespace AppendStore
{

/// base exception class of all below exceptions, more exceptions will be added.
class AppendStoreExceptionBase : public apsara::ExceptionBase
{
public:
    AppendStoreExceptionBase(const std::string& message = "") throw ()
        : ExceptionBase(message)
    {}
};

class AppendStoreWriteException : public AppendStoreExceptionBase
{
public:
    AppendStoreWriteException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreReadException : public AppendStoreExceptionBase
{
public:
    AppendStoreReadException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreInvalidIndexException : public AppendStoreExceptionBase
{
public:
    AppendStoreInvalidIndexException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreInvalidChunkException : public AppendStoreExceptionBase
{
public:
    AppendStoreInvalidChunkException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreCompressionException : public AppendStoreExceptionBase
{
public:
    AppendStoreCompressionException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreCodecException : public AppendStoreExceptionBase
{
public:
    AppendStoreCodecException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreMisMatchedVerException: public AppendStoreExceptionBase
{
public:
    AppendStoreMisMatchedVerException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreNotExistException: public AppendStoreExceptionBase
{
public:
    AppendStoreNotExistException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

class AppendStoreFactoryException: public AppendStoreExceptionBase
{
public:
    AppendStoreFactoryException(const std::string& message = "") throw ()
        : AppendStoreExceptionBase(message)
    {}
};

}
}

#endif
