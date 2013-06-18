/*
 * Writes a snapshot into append store
 * Usage: snapshot_write sample_data current_trace [parent_trace]
 */
#include <iostream>
#include <cstdlib>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include "../include/store.h"
#include "../include/exception.h"
#include "../append-store/append_store_types.h"
#include "../append-store/append_store.h"
#include "../fs/qfs_file_system_helper.h"
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

LoggerPtr ss_write_logger(Logger::getLogger("BigArchive.Snapshot.Write"));

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

PanguAppendStore* init_as_write(string& vm_id)
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
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store for write" << e.ToString());
    }
    catch (...) {
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store for write");
    }
    return NULL;
}

PanguAppendStore* init_as_read(string& vm_id)
{
    try {
        PanguAppendStore *pas = NULL;
        StoreParameter sp = StoreParameter(); ;
        string store_name = "/" + kBasePath + "/" + vm_id + "/" + "append";
        sp.mPath = store_name;
        sp.mAppend = false;
        pas = new PanguAppendStore(sp, false);
        return pas;
    }
    catch (ExceptionBase& e) {
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store for read" << e.ToString());
    }
    catch (...) {
        LOG4CXX_ERROR(ss_write_logger, "Couldn't init append store for read");
    }
    return NULL;
}


int main(int argc, char *argv[]) {
    signal(SIGSEGV, crash_handler);

	if(argc != 3 && argc != 4) { 
		cout << "Usage: " << argv[0] << " sample_data current_trace [parent_trace]" << endl;
		return -1;
	}

    DOMConfigurator::configure("Log4cxxConfig.xml");
    QFSHelper::Connect();
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
    current->InitBloomFilters(ds.GetSnapshotSize());

    // 1. init append store
    // TODO: change appendstore factory to singleton
    PanguAppendStore* pas = init_as_write(current->ss_meta_.vm_id_);
    if (pas == NULL) {
        LOG4CXX_ERROR(ss_write_logger, "Unable to init append store");
        return -1;
    }
    current->SetAppendStore(pas);

    // 2. load parent snapshot meta from qfs, init current snapshot meta
    if (has_parent) {
        //pas = init_as_read(current->ss_meta_.vm_id_);
        parent->SetAppendStore(pas);
        parent->LoadSnapshotMeta();
    }

    // 3. for every loaded segment, do
    uint64_t l1_blocks = 0, l1_size = 0, l2_blocks = 0, l2_size = 0, 
        l3_size = 0, l3_blocks = 0, new_blocks = 0, new_size = 0, tot_blocks = 0, tot_size = 0;
    uint64_t last_pos = 0;
    SegmentMeta cur_seg, par_seg;
    vector<SegmentMeta> seg_meta_buf;
    BlockMeta* bm;
    CdsIndex cds_index;
    int num_queries = 0;	// number of CDS queries
    uint32_t seg_id = 0;	// numberical id of segment
    Checksum* cksums = new Checksum[BATCH_QUERY_BUFFER_SIZE];
    bool* results = new bool[BATCH_QUERY_BUFFER_SIZE];
    BlockMeta** blks_to_query = new BlockMeta* [BATCH_QUERY_BUFFER_SIZE];
    uint64_t* offsets = new uint64_t[BATCH_QUERY_BUFFER_SIZE];
    while (ds.GetSegment(cur_seg)) {
        // data generator does not calculate the offset of segment
        last_pos += cur_seg.size_;
        cur_seg.end_offset_ = last_pos;

        num_queries = 0;
        current->UpdateBloomFilters(cur_seg);
        tot_blocks += cur_seg.segment_recipe_.size(); tot_size += cur_seg.size_;	// stat total
        if (has_parent && parent->LoadSegmentRecipe(par_seg, seg_id++)) {
            //  a) first compare parent segment meta by cksum
            if (cur_seg.cksum_ == par_seg.cksum_) {
                cur_seg.handle_ = par_seg.handle_;
                current->UpdateSnapshotRecipe(cur_seg);
                //l1_blocks += par_seg.segment_recipe_.size(); l1_size += par_seg.size_;	// stat l1
                l1_blocks += cur_seg.segment_recipe_.size(); l1_size += cur_seg.size_;	// stat l1
                LOG4CXX_INFO(ss_write_logger, "parent segment size: " << par_seg.size_
                             << " current segment size: " << cur_seg.size_);
                assert(par_seg.size_ == cur_seg.size_);
                continue;
            }

            //  b) then compare block by hash
            par_seg.BuildIndex();
            for (size_t i = 0; i < cur_seg.segment_recipe_.size(); ++i)
            {
                bm = par_seg.SearchBlock(cur_seg.segment_recipe_[i].cksum_);
                if (bm != NULL) {
                    cur_seg.segment_recipe_[i].handle_ = bm->handle_;
                    cur_seg.segment_recipe_[i].flags_ = bm->flags_ | IN_PARENT;
                    l2_blocks += 1; l2_size += cur_seg.segment_recipe_[i].size_;	// stat l2
                }
                else {
                    // these blocks are not found in parent snapshot's segment, will ask CDS
                    cksums[num_queries] = cur_seg.segment_recipe_[i].cksum_;
                    blks_to_query[num_queries++] = &cur_seg.segment_recipe_[i];
                }
            }
        }
        else {
            // if there's no parent, then we can only ask CDS
            for (size_t i = 0; i < cur_seg.segment_recipe_.size(); ++i) {
                cksums[num_queries]= cur_seg.segment_recipe_[i].cksum_;
                blks_to_query[num_queries++] = &cur_seg.segment_recipe_[i];
            }
        }
        //  c) check with cds
        cds_index.BatchGet(cksums, num_queries, results, offsets);
        for (int i = 0; i < num_queries; i++) {
            if (results[i] == true) {
                // if found in CDS, it should return the data offset in CDS data file
                blks_to_query[i]->handle_ = offsets[i];
                blks_to_query[i]->flags_ |= IN_CDS;
                l3_blocks += 1; l3_size += blks_to_query[i]->size_;	// stat l3
            }
            //  d) write new data to append store
            else {
                current->SaveBlockData(*blks_to_query[i]);
                new_blocks += 1; new_size += blks_to_query[i]->size_;	// stat final write
            }
        }
        
        //  e) write segment recipe
        // to make segment meta data placed sequencially on disk, we buffer it and write in batch mode
        /*
        seg_meta_buf.push_back(cur_seg);
        if (seg_meta_buf.size() >= DF_MAX_PENDING) {
            for (size_t i = 0; i < seg_meta_buf.size(); i++) {
                current->SaveSegmentRecipe(seg_meta_buf[i]);
                current->UpdateSnapshotRecipe(seg_meta_buf[i]);
            }
            seg_meta_buf.clear();
            }*/
        current->SaveSegmentRecipe(cur_seg);
        current->UpdateSnapshotRecipe(cur_seg);
    }

    // 4. write snapshot meta to qfs
    current->SaveSnapshotMeta();
    current->SaveBloomFilters();
	pas->Flush();
	pas->Close();

    LOG4CXX_INFO(ss_write_logger, "total: " << tot_blocks << " " << tot_size);
    LOG4CXX_INFO(ss_write_logger, "l1: " << l1_blocks << " " << l1_size);
    LOG4CXX_INFO(ss_write_logger, "l2: " << l2_blocks << " " << l2_size);
    LOG4CXX_INFO(ss_write_logger, "l3: " << l3_blocks << " " << l3_size);
    LOG4CXX_INFO(ss_write_logger, "new: " << new_blocks << " " << new_size);

    delete pas;
    delete[] cksums;
    delete[] results;
    delete[] offsets;
    delete[] blks_to_query;
	return 0;
}











