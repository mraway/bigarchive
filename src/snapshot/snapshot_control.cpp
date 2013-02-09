#include "snapshot_control.h"
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

LoggerPtr sc_logger(Logger::getLogger("SnapshotControl"));

SnapshotControl::SnapshotControl(const string& trace_file)
{
    trace_file_ = trace_file;
    parse_trace_file();
    store_path_ = "/" + append_store_base_path + "/" + vm_id_ + "/" + "append";
    ss_meta_filename_ = "/" + append_store_base_path + "/" + vm_id_ + "/" + snapshot_id_;
}

void SnapshotControl::SetAppendStore(PanguAppendStore* pas)
{
    pas_ = pas;
}

void SnapshotControl::ParseTraceFile(const string& trace_file)
{
    size_t name_seperator = trace_file.rfind('/');
    size_t disk_seperator = trace_file.rfind('/', name_seperator - 1);
    size_t os_seperator = trace_file.rfind('/', disk_seperator - 1);
    size_t vm_seperator = trace_file.find('.', name_seperator);
    size_t ss_seperator = trace_file.find_last_of('-');
    os_type = trace_file.substr(os_seperator + 1, disk_seperator - os_seperator - 1);
    disk_type = trace_file.substr(disk_seperator + 1, name_seperator - disk_seperator - 1);
    vm_id = trace_file.substr(name_seperator + 1, vm_seperator - name_seperator - 1);
    ss_id = trace_file.substr(vm_seperator + 1, ss_seperator - vm_seperator - 1);
    LOG4CXX_INFO(sc_logger, "trace: " << trace_file);
    LOG4CXX_INFO(sc_logger, "os: " << os_type);
    LOG4CXX_INFO(sc_logger, "disk: " << disk_type);
    LOG4CXX_INFO(sc_logger, "vm: " << vm_id);
    LOG4CXX_INFO(sc_logger, "snapshot: " << ss_id);
}




