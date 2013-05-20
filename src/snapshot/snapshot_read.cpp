/*
 * Reads a snapshot from append store to local disk,
 * If a current VM disk already exist, then we can avoid reading data
 * from append store by copying duplicate segments from existing VM image.
 * Usage: snapshot_read sample_data output_file snapshot_trace [current_trace]
 */

#include <iostream>

#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include "snapshot_control.h"
#include "cds_data.h"
#include "../fs/qfs_file_system_helper.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

LoggerPtr logger(Logger::getLogger("Snapshot_read"));

PanguAppendStore* init_append_store(string& vm_id)
{
    try {
        PanguAppendStore *pas = NULL;
        StoreParameter sp = StoreParameter(); ;
        string store_name = "/" + kBasePath + "/" + vm_id + "/" + "append";
        sp.mPath = store_name;
        sp.mAppend = false;	// write only
        pas = new PanguAppendStore(sp, false);	// do not create
        return pas;
    }
    catch (ExceptionBase& e) {
        LOG4CXX_ERROR(logger, "Couldn't init append store" << e.ToString());
    }
    catch (...) {
        LOG4CXX_ERROR(logger, "Couldn't init append store");
    }
    return NULL;
}

int main(int argc, char** argv)
{
    if (argc != 5 && argc != 6) {
        cout << "Usage: " << argv[0] << " cds_name sample_data output_file snapshot_trace [current_trace]" << endl;
        return -1;
    }

    DOMConfigurator::configure("Log4cxxConfig.xml");

    string cds_name(argv[1]);
    string sample_file(argv[2]);
    string data_file(argv[3]);
    string snapshot_trace(argv[4]);
    string current_trace;
    bool has_current = false;
    if (argc == 6) {
        current_trace = argv[5];
        has_current = true;
    }

    // output snapshot file
    ofstream output(data_file, ios::out | ios::binary | ios::trunc);

    // init file system
    QFSHelper::Connect();

    // some data come from current VM image
    DataSource* pds = NULL;
    if (has_current) {
        pds = new DataSource(current_trace, sample_file);
    }

    // init cds data
    CdsData cds_data(cds_name, kCdsDataOptions);

    // init snapshot controller for IO with QFS
    SnapshotControl ssctrl(snapshot_trace);
    // init append store for data IO
    PanguAppendStore* pas = init_append_store(ssctrl.ss_meta_.vm_id_);
    if (pas == NULL) {
        LOG4CXX_ERROR(logger, "Unable to init append store");
        return -1;
    }
    ssctrl.SetAppendStore(pas);

    if (!ssctrl.LoadSnapshotMeta())
        return -1;

    SegmentMeta cur_seg, ss_seg;
    uint32_t size_read;
    for (size_t segid = 0; segid < ssctrl.ss_meta_.snapshot_recipe_.size(); segid++) {
        // 1. compare with the current segment on disk
        if (has_current && pds->GetSegment(cur_seg)) {
            if (ssctrl.ss_meta_.snapshot_recipe_[segid].cksum_ == cur_seg.cksum_) {
                // write cur_seg data to disk
                for (size_t blkid = 0; blkid < cur_seg.segment_recipe_.size(); blkid++)
                    cur_seg.segment_recipe_[blkid].SerializeData(output);
                continue;
            }
        }
        // 2. load block data from append store and CDS
        ssctrl.LoadSegmentRecipe(ss_seg, segid);
        for (size_t blkid = 0; blkid < ss_seg.segment_recipe_.size(); blkid++) {
            if (ss_seg.segment_recipe_[blkid].flags_ & IN_CDS) {
                size_read = cds_data.Read(ss_seg.segment_recipe_[blkid]);
                if (size_read != ss_seg.segment_recipe_[blkid].size_) {
                    LOG4CXX_ERROR(logger, "Read CDS data fail");
                    return -1;
                }
            }
            else {
                if (!ssctrl.LoadBlockData(ss_seg.segment_recipe_[blkid])) {
                    LOG4CXX_ERROR(logger, "Read append store data fail");
                    return -1;
                }
            }
        }
        // 3. save data to disk
        for (size_t blkid = 0; blkid < ss_seg.segment_recipe_.size(); blkid++)
            ss_seg.segment_recipe_[blkid].SerializeData(output);
    }

    output.close();
    if (pds != NULL)
        delete pds;
}










