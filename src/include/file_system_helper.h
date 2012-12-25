/*
File System helper header
*/

#ifndef FILE_SYSTEM_HELPER_H
#define FILE_SYSTEM_HELPER_H

#include<vector>
#include<string>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// static logger variable
LoggerPtr logger(Logger::getLogger( "appendstore.qfs_helper"));
 //

using std::vector;
using std::string;

class FileSystemHelper {
 public:
  /* Connect method establishes a connection with FileSystem MetaServer (usually host name and port are passed)*/
  void Connect() {}
  void Connect(string host, int port) {}
  /* list file contents */
  int ListDir(string pathname, vector<string> &result) {}
  /* checks whether file "fname" exists or not */
  bool IsFileExists(string fname) {}
  /* checks whether directory "dirname" exits or not */
  bool IsDirectoryExists(string dirname) {}
  /* gets file Size */
  int getSize(string fname) {}
  /* create Directory */
  int CreateDirectory(string dirname) {}
};

#endif /* FILE_SYSTEM_HELPER_H */
