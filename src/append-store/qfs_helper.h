#ifndef _PANGU_HELPER_H
#define _PANGU_HELPER_H

#include <string>
#include "append_store_exception.h"

  class QFSHelper 
  {
    public: 
     static int64_t GetFileSize(const std::string& fname);
     static void CreateFile(const std::string& fname, int min, int max, const std::string& appName, const std::string& partName);
     static void CreateLogFile(const std::string& fname, int min, int max, const std::string& appname, const std::string& partname);
     static bool IsFileExist(const std::string& fname);
     static bool IsDirectoryExist(const std::string& dirname);

     /*
      * check with Wei
      */
	
     // static pangu::LogFileOutputStreamPtr OpenLog4Append(const std::string& fname);
     static ofstream OpenLog4Append(const std::string& fname);
     
     // static pangu::LogFileInputStreamPtr OpenLog4Read(const std::string& fname);
     static ifstream OpenLog4Read(const std::string& fname);

     // static bool ReleaseLogFileLock(const std::string& fname);
     static bool ReleaseLogFileLock(const std::string& fname);
  };

#endif
