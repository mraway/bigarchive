/*
 * Writes a snapshot into append store
 * Usage: snapshot_write sample_data snapshot_trace parent_snapshot_trace
 */
#include <iostream>
#include <cstdlib>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include "../include/store.h"
#include "../append-store/append_store_types.h"
#include "../append-store/append_store.h"
#include "data_source.h"
#include "../exception/exception.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

LoggerPtr ss_write_logger(Logger::getLogger("Snapshot_write"));

string ROOT_DIRECTORY = "root";

void parse_trace_file(const string& trace_file, string& os_type, string& disk_type, string& vm_id, string& ss_id)
{
    size_t name_seperator = trace_file.rfind('/');
    size_t disk_seperator = trace_file.rfind('/', name_seperator);
    size_t os_seperator = trace_file.rfind('/', disk_seperator);
    size_t vm_seperator = trace_file.find('.', name_seperator);
    size_t ss_seperator = trace_file.find_last_of('-');
    os_type = trace_file.substr(os_seperator + 1, disk_seperator - os_seperator);
    disk_type = trace_file.substr(disk_seperator + 1, name_seperator - disk_seperator);
    vm_id = trace_file.substr(name_seperator + 1, vm_seperator - name_seperator);
    ss_id = trace_file.substr(vm_seperator + 1, ss_seperator - vm_seperator);
    LOG4CXX_INFO(ss_write_logger, "trace: " << trace_file);
    LOG4CXX_INFO(ss_write_logger, "os: " << os_type);
    LOG4CXX_INFO(ss_write_logger, "disk: " << disk_type);
    LOG4CXX_INFO(ss_write_logger, "vm: " << vm_id);
    LOG4CXX_INFO(ss_write_logger, "snapshot: " << ss_id);
}

PanguAppendStore* init_append_store(string trace_file)
{
    try {
        PanguAppendStore *pas = NULL;
        StoreParameter sp = StoreParameter(); ;
        string os_type, disk_type, vm_id, ss_id;
        parse_trace_file(trace_file, os_type, disk_type, vm_id, ss_id);
        string store_name = ROOT_DIRECTORY + "/" + vm_id + "/" + "append";
        sp.mPath = store_name;
        sp.mAppend = true;
        pas = new PanguAppendStore(sp, true);
        return pas;
    }
    catch (ExceptionBase& e) {
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store" << e.ToString());
    }
    return NULL;
}

int main(int argc, char *argv[]) {
	if(argc != 3 && argc != 4) { 
		cout << "Usage: snapshot_write sample_data snapshot_trace parent_snapshot_trace" << endl;
		return -1;
	}

	string sample_file(argv[1]);
	string snapshot_file(argv[2]);
    string parent_file;
    bool has_parent = false;
    if (argc == 4) {
        parent_file = argv[3];
        has_parent = true;
    }

    PanguAppendStore* pas = init_append_store(snapshot_file);
	pas->Flush();
	pas->Close();

    delete pas;

	return 0;
}

