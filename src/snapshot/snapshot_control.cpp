#include "snapshot_control.h"

LoggerPtr SnapshotControl::logger_ = Logger::getLogger("SnapshotControl");

SnapshotControl::SnapshotControl(const string& trace_file)
{
    trace_file_ = trace_file;
    ParseTraceFile();
    store_path_ = "/" + base_path + "/" + ss_meta_.vm_id_ + "/" + "append";
    ss_meta_pathname_ = "/" + base_path + "/" + ss_meta_.vm_id_ + "/" + ss_meta_.snapshot_id_;
    seg_pos_ = 0;
    ss_meta_.size_ = 0;
}

void SnapshotControl::SetAppendStore(PanguAppendStore* pas)
{
    pas_ = pas;
}

void SnapshotControl::ParseTraceFile()
{
    size_t name_seperator = trace_file_.rfind('/');
    size_t disk_seperator = trace_file_.rfind('/', name_seperator - 1);
    size_t os_seperator = trace_file_.rfind('/', disk_seperator - 1);
    size_t vm_seperator = trace_file_.find('.', name_seperator);
    size_t ss_seperator = trace_file_.find_last_of('-');
    os_type_ = trace_file_.substr(os_seperator + 1, disk_seperator - os_seperator - 1);
    disk_type_ = trace_file_.substr(disk_seperator + 1, name_seperator - disk_seperator - 1);
    ss_meta_.vm_id_ = trace_file_.substr(name_seperator + 1, vm_seperator - name_seperator - 1);
    ss_meta_.snapshot_id_ = trace_file_.substr(vm_seperator + 1, ss_seperator - vm_seperator - 1);
    LOG4CXX_INFO(logger_, "trace: " << trace_file_ << " os: " << os_type_ 
                 << " disk: " << disk_type_ << " vm: " << ss_meta_.vm_id_ << " snapshot: " << ss_meta_.snapshot_id_);
}

bool SnapshotControl::LoadSnapshotMeta()
{
    string ss_pathname = "root/" + ss_meta_.vm_id_ + "/" + ss_meta_.snapshot_id_;

    // open the snapshot meta file in qfs and read it
    // TODO: this connect uses fix ip address
	QFSHelper fsh; 
    fsh.Connect();
	if (!fsh.IsFileExists(ss_pathname)) {
        LOG4CXX_ERROR(logger_, "Couldn't find snapshot meta: " << ss_meta_.vm_id_ << " " << ss_meta_.snapshot_id_);
		return false;
	}
	QFSFileHelper fh(&fsh, ss_pathname, O_RDONLY);
	fh.Open();
	int read_length = fh.GetNextLogSize();
	char *data = new char[read_length];
	fh.Read(data, read_length);
	fh.Close();
	fsh.DisConnect();
    LOG4CXX_INFO(logger_, "Read " << read_length << " from file");

    stringstream buffer;
    buffer.write(data, read_length);
    ssmeta_.Deserialize(buffer);
    ssmeta_.DeserializeRecipe(buffer);
    LOG4CXX_INFO(logger_, "Snapshot meta loaded: " << ssmeta_.vm_id_ << " " << ssmeta_.snapshot_id_);

    delete[] data;
    return true;
}

bool SnapshotControl::SaveSnapshotMeta()
{
	QFSHelper fsh;
	fsh.Connect();

	string ss_path = "root/" + ss_meta_.vm_id_;
    if (!fsh.IsDirectoryExists(ss_path))
        fsh.CreateDirectory(ss_path);
    string ss_pathname = ss_path + "/" + ss_meta_.snapshot_id_;
    if (fsh.IsFileExists(ss_pathname))
        fsh.RemoveFile(ss_pathname);
	QFSFileHelper fh(&fsh, ss_pathname, O_WRONLY);
	fh.Create();

	stringstream buffer;
	ssmeta_.Serialize(buffer);
    ssmeta_.SerializeRecipe(buffer);
	LOG4CXX_INFO(logger_, "Snapshot meta size " << buffer.str().size());
    fh.Write((char *)buffer.str().c_str(), buffer.str().size());
    fh.Close();
    return true;
}

bool SnapshotControl::LoadSegmentRecipe(SegmentMeta& sm)
{
    string data;
    string handle((char*)&sm.handle_, sizeof(sm.handle_));
    pas_->Read(handle, &data);
    LOG4CXX_INFO(logger_, "Read segment meta : " << data.size());
    
    stringstream ss(data);
    sm.DeserializeRecipe(ss);
    return true;
}

bool SnapshotControl::SaveSegmentRecipe(SegmentMeta& sm)
{
    stringstream buffer;
    sm.SerializeRecipe(buffer);
    string handle = pas_->Append(buffer.str());
    sm.SetHandle(handle);
    return true;
}

bool SnapshotControl::SaveBlockData(BlockMeta& bm)
{
    string data(bm.data_, bm.size_);
    string handle = pas_->Append(data);
    bm.SetHandle(handle);
    bm.flags_ |= IN_AS;
    return true;
}














