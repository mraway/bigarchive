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
#include "cds_index.h"
#include <execinfo.h>
#include <signal.h>

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

#define BATCH_QUERY_BUFFER_SIZE 2048

LoggerPtr ss_write_logger(Logger::getLogger("Snapshot_write"));

void crash_handler(int sig) {
    void *array[10];
    size_t size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    fprintf(stderr, "Error: signal %d:\n", sig);
    backtrace_symbols_fd(array, size, 2);
    exit(1);
}

PanguAppendStore* init_append_store(string& vm_id)
{
    try {
        PanguAppendStore *pas = NULL;
        StoreParameter sp = StoreParameter(); ;
        string store_name = "/" + kBasePath + "/" + vm_id + "/" + "append";
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
    signal(SIGSEGV, crash_handler);

	if(argc != 3 && argc != 4) { 
		cout << "Usage: " << argv[0] << " sample_data snapshot_trace parent_snapshot_trace" << endl;
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


    DataSource ds(snapshot_file, sample_file);
    SnapshotControl* current = NULL;
    SnapshotControl* parent = NULL;
    current = new SnapshotControl(snapshot_file);
    if (has_parent) {
        parent = new SnapshotControl(parent_file);
        if (current->os_type_ != parent->os_type_ || 
            current->disk_type_ != parent->disk_type_ || 
            current->ss_meta_.vm_id_ != parent->ss_meta_.vm_id_) {
            LOG4CXX_ERROR(ss_write_logger, 
                          "Current snapshot and parent snapshot belong to different VM");
            return -1;
        }
    }

    // 1. init append store
    // TODO: change appendstore factory to singleton
    PanguAppendStore* pas = init_append_store(current->ss_meta_.vm_id_);
    if (pas == NULL) {
        LOG4CXX_ERROR(ss_write_logger, "Unable to init append store");
        return -1;
    }
    current->SetAppendStore(pas);

    // 2. load parent snapshot meta from qfs, init current snapshot meta
    if (has_parent) {
        parent->SetAppendStore(pas);
        parent->LoadSnapshotMeta();
    }

    // 3. for every loaded segment, do
    SegmentMeta cur_seg, par_seg;
    BlockMeta* bm;
    CdsIndex cds_index;
    int num_queries = 0;
    Checksum* cksums = new Checksum[BATCH_QUERY_BUFFER_SIZE];
    bool* results = new bool[BATCH_QUERY_BUFFER_SIZE];
    BlockMeta** blks_to_query = new BlockMeta* [BATCH_QUERY_BUFFER_SIZE];
    uint64_t* offsets = new uint64_t[BATCH_QUERY_BUFFER_SIZE];
    while (ds.GetSegment(cur_seg)) {
        num_queries = 0;
        current->UpdateBloomFilters(cur_seg);

        //  a) load and compare parent segment meta by cksum
        if (has_parent) {
            parent->LoadSegmentRecipe(par_seg);
            if (cur_seg.cksum_ == par_seg.cksum_) {
                cur_seg.handle_ = par_seg.handle_;
                continue;
            }

            //  b) compare block by hash
            par_seg.BuildIndex();
            for (size_t i = 0; i < cur_seg.segment_recipe_.size(); ++i)
            {
                bm = par_seg.SearchBlock(cur_seg.segment_recipe_[i].cksum_);
                if (bm != NULL) {
                    cur_seg.segment_recipe_[i].handle_ = bm->handle_;
                    cur_seg.segment_recipe_[i].flags_ = bm->flags_ | IN_PARENT;
                }
                else {
                    cksums[num_queries] = cur_seg.segment_recipe_[i].cksum_;
                    blks_to_query[num_queries++] = &cur_seg.segment_recipe_[i];
                }
            }
        }
        else {
            for (size_t i = 0; i < cur_seg.segment_recipe_.size(); ++i) {
                cksums[num_queries]= cur_seg.segment_recipe_[i].cksum_;
                blks_to_query[num_queries++] = &cur_seg.segment_recipe_[i];
            }
        }
        //  c) check with cds
        cds_index.BatchGet(cksums, num_queries, results, offsets);
        for (int i = 0; i < num_queries; i++) {
            if (results[i] == true) {
                blks_to_query[i]->handle_ = offsets[i];
                blks_to_query[i]->flags_ |= IN_CDS;
            }
            //  d) write new data
            else {
                current->SaveBlockData(*blks_to_query[i]);
            }
        }
        
        //  e) write segment recipe
        current->SaveSegmentRecipe(cur_seg);
    }

    // 4. write snapshot meta to qfs
    current->SaveSnapshotMeta();
    current->SaveBloomFilters();
	pas->Flush();
	pas->Close();

    delete pas;
    delete[] cksums;
    delete[] results;
    delete[] offsets;
    delete[] blks_to_query;
	return 0;
}











