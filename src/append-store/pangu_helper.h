#ifndef _PANGU_HELPER_H
#define _PANGU_HELPER_H

#include <string>
#include "apsara/pangu.h"
#include "append_store_exception.h"

namespace apsara
{
namespace AppendStore
{

class PanguHelper 
{
public: 
    static int64_t GetFileSize(const std::string& fname);

    /* 
     * make sure InitPangu() is called before using this function
    */
    static void CreateFile(const std::string& fname, int min, int max, const std::string& appName, const std::string& partName);

    static void CreateLogFile(const std::string& fname, int min, int max, const std::string& appname, const std::string& partname);

    /* 
     * make sure InitPangu() is called before using this function
    */
    static bool IsFileExist(const std::string& fname);

    static bool IsDirectoryExist(const std::string& dirname);

    static pangu::LogFileOutputStreamPtr OpenLog4Append(const std::string& fname);

    static pangu::LogFileInputStreamPtr OpenLog4Read(const std::string& fname);

    static bool ReleaseLogFileLock(const std::string& fname);
};

}
}

#endif
