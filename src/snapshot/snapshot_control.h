#ifndef _VM_SNAPSHOT_STORE_H_
#define _VM_SNAPSHOT_STORE_H_
#include <iostream>
#include <cstdlib>
#include "../include/store.h"
#include "../append-store/append_store_types.h"
#include "../append-store/append_store.h"
#include "data_source.h"
#include "../exception/exception.h"

using namespace std;

static string append_store_base_path = "root";

class SnapshotControl {
public:
    SnapshotControl(const string& trace_file);
    ~SnapshotControl();
    bool LoadSnapshotMeta();
    bool SaveSnapshotMeta();
    bool LoadSegmentMeta();
    bool SaveSegmentMeta();
    void SetAppendStore(PanguAppendStore* pas);
public:
    string trace_file_;
    string os_type_;
    string disk_type_;
    string vm_id_;
    string snapshot_id_;
    string store_path_;
    string ss_meta_filename_;
    SnapshotMeta ss_meta_;
private:
    void ParseTraceFile(const string& trace_file);
private:
    PanguAppendstore* pas_;
    SnapshotMeta ssmeta_;
};


#endif // _VM_SNAPSHOT_STORE_H_




















