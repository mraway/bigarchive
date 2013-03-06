/*
 * Snapshot controller manages a snapshot data and all corresponding io operations
 */

#ifndef _SNAPSHOT_CONTROL_H_
#define _SNAPSHOT_CONTROL_H_
#include <iostream>
#include <cstdlib>
#include "../include/store.h"
#include "../include/exception.h"
#include "../append-store/append_store_types.h"
#include "../append-store/append_store.h"
#include "data_source.h"
#include "../fs/qfs_file_helper.h"
#include "../fs/qfs_file_system_helper.h"
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>
#include "bloom_filter.h"
#include "bloom_filter_functions.h"

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;
using namespace std;

extern const string kBasePath;

class SnapshotControl {
public:
    /*
     * Constructor, either from a trace file name or provide snapshot meta information
     */
    SnapshotControl(const string& trace_file);
    SnapshotControl(const string& os_type, const string& disk_type, const string& vm_id, const string& ss_id);

    ~SnapshotControl();

    /*
     * Save or load snapshot meta data (include recipe) from file system
     */
    bool LoadSnapshotMeta();
    bool SaveSnapshotMeta();

    /*
     * Save or load one segment recipe from append store
     */
    bool LoadSegmentRecipe(SegmentMeta& sm);
    bool SaveSegmentRecipe(SegmentMeta& sm);

    /*
     * Save or load one block data from append store
     */
    bool SaveBlockData(BlockMeta& bm);

    /*
     * Set an initialized append store pointer
     */
    void SetAppendStore(PanguAppendStore* pas);

    /*
     * Create two bloom filters, the settings should come from a snapshot config file in QFS,
     * but if such config doesn't exist, it will create one base on current snapshot size
     */
    bool InitBloomFilters(uint64_t snapshot_size);
    /*
     * Save, load or remove snapshot's bloom filters
     */
    bool SaveBloomFilters();
    bool SaveBloomFilter(BloomFilter<Checksum>* pbf, const string& bf_name);
    bool LoadBloomFilter(BloomFilter<Checksum>* pbf, const string& bf_name);
    bool RemoveBloomFilters();
    bool RemoveBloomFilter(const string& bf_name);

    /*
     * Add elements (checksums) in segment recipe to bloom filters
     */
    void UpdateBloomFilters(const SegmentMeta& sm);

public:
    string trace_file_;
    string os_type_;
    string disk_type_;
    string store_path_;
    string ss_meta_pathname_;
    string vm_path_;
    string vm_meta_pathname_;
    string primary_filter_pathname_;
    string secondary_filter_pathname_;
    SnapshotMeta ss_meta_;
    VMMeta vm_meta_;

private:
    void Init();
    void ParseTraceFile();

private:
    PanguAppendStore* store_ptr_;
    BloomFilter<Checksum>* primary_filter_ptr_;
    BloomFilter<Checksum>* secondary_filter_ptr_;
    SnapshotMeta ssmeta_;
    static LoggerPtr logger_;
    size_t seg_pos_;
};

#endif // _SNAPSHOT_CONTROL_H_
