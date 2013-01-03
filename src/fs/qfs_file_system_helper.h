#include "file_system_helper.h"
#include "KfsClient.h"
#include "exception.h"

/*
  #include <log4cxx/logger.h>
  #include <log4cxx/xml/domconfigurator.h>

  using namespace log4cxx;
  using namespace log4cxx::xml;
  using namespace log4cxx::helpers;

  // static logger variable
  LoggerPtr logger(Logger::getLogger( "appendstore"));
*/

using std::string;

class QFSHelper : public FileSystemHelper {

public:
    void Connect(); 
    void Connect(string metaserverhost, int metaserverport);
    bool IsFileExists(string fname);
    bool IsDirectoryExists(string dirname);
    int getSize(string fname);
    int ListDir(string pathname, vector<string> &result);
    int CreateDirectory(string pathname);

public:	
    KFS::KfsClient *kfsClient;
};
