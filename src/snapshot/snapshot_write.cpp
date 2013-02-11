/*
 * Writes a snapshot into append store
 * Usage: snapshot_write sample_data snapshot_trace parent_snapshot_trace
 */
#include <iostream>
#include <cstdlib>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include "../include/store.h"
#include "../include/exception.h"
#include "../append-store/append_store_types.h"
#include "../append-store/append_store.h"
#include "data_source.h"
#include "snapshot_control.h"
#include "snapshot_types.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

LoggerPtr ss_write_logger(Logger::getLogger("Snapshot_write"));

PanguAppendStore* init_append_store(string& vm_id)
{
    try {
        PanguAppendStore *pas = NULL;
        StoreParameter sp = StoreParameter(); ;
        string store_name = "/" + ROOT_DIRECTORY + "/" + vm_id + "/" + "append";
        sp.mPath = store_name;
        sp.mAppend = true;
        pas = new PanguAppendStore(sp, true);
        return pas;
    }
    catch (ExceptionBase& e) {
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store" << e.ToString());
    }
    catch (...) {
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store");
    }
    return NULL;
}

int main(int argc, char *argv[]) {
	if(argc != 3 && argc != 4) { 
		cout << "Usage: %s sample_data snapshot_trace parent_snapshot_trace" << argv[0] << endl;
		return -1;
	}

    DOMConfigurator::configure("Log4cxxConfig.xml");
	string sample_file(argv[1]);
	string snapshot_file(argv[2]);
    string parent_file;
    bool has_parent = false;
    if (argc == 4) {
        parent_file = argv[3];
        has_parent = true;
    }
    
    SnapshotControl* current = NULL;
    SnapshotControl* parent = NULL;
    current = new SnapshotControl(snapshot_file);
    if (has_parent) {
        parent = new SnapshotControl(parent_file);
        if (current->os_type_ != parent->os_type_ || 
            current->disk_type_ != parent->disk_type_ || 
            current->vm_id_ != parent->vm_id_) {
            LOG4CXX_ERROR(ss_write_logger, 
                          "Current snapshot and parent snapshot belong to different VM");
            return -1;
        }
    }

    // 1. init append store
    // TODO: change appendstore factory to singleton
    PanguAppendStore* pas = init_append_store(current->vm_id_);
    if (pas == NULL) {
        LOG4CXX_ERROR(ss_write_logger, "Unable to init append store");
        return -1;
    }
    current->SetAppendStore(pas);
    parent->SetAppendStore(pas);

    // 2. load parent snapshot meta from qfs, init current snapshot meta
    parent->LoadSnapshotMeta();
    current->InitSnapshotMeta();

    // 3. init data source
    DataSource ds(snapshot_file, sample_file);

    // 4. for every loaded segment, do
    //  d) write new data
    //  e) write segment meta
    SegmentMeta current_segment, parent_segment;
    HandleType handle;
    CdsIndex cds_index;
    while (ds.GetSegment(current_segment)) {
        //  a) load and compare parent segment meta by cksum
        parent->LoadSegmentMeta(par_sm);
        if (cur_seg.cksum_ == par_seg.cksum_) {
            cur_seg.Copy(par_sm);
            continue;
        }
        //  b) compare block by hash
        for (size_t i = 0; i < cur_seg.block_list_.size(); ++i)
        {
            handle = par_seg.SearchBlock(cur_seg.block_list_[i].cksum_);
            if (handle != 0)
                cur_seg.block_list_[i].handle_ = handle;
            else
                p_cksums[j++] = cur_seg.block_list_[i].cksum_;
        }
        //  c) check with cds
        for (size_t i = 0; i < cur_seg.block_list_.size(); ++i)
        {
            if (curhandle_
        cds_index.Get(
    }

    // 5. write snapshot meta to qfs
    if (has_parent)
        parent->SaveSnapshotMeta();
	pas->Flush();
	pas->Close();

    delete pas;

	return 0;
}











