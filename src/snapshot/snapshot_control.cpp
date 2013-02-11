#include "snapshot_control.h"

LoggerPtr SnapshotControl::logger_ = Logger::getLogger("SnapshotControl");

SnapshotControl::SnapshotControl(const string& trace_file)
{
    trace_file_ = trace_file;
    ParseTraceFile();
    store_path_ = "/" + append_store_base_path + "/" + vm_id_ + "/" + "append";
    ss_meta_filename_ = "/" + append_store_base_path + "/" + vm_id_ + "/" + snapshot_id_;
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
    vm_id_ = trace_file_.substr(name_seperator + 1, vm_seperator - name_seperator - 1);
    snapshot_id_ = trace_file_.substr(vm_seperator + 1, ss_seperator - vm_seperator - 1);
    LOG4CXX_INFO(logger_, "trace: " << trace_file_ << " os: " << os_type_ 
                 << " disk: " << disk_type_ << " vm: " << vm_id_ << " snapshot: " << snapshot_id_);
}

bool SnapshotControl::LoadSnapshotMeta()
{
    string ss_pathname = "root/" + vm_id_ + "/" + snapshot_id_;

    // open the snapshot meta file in qfs and read it
    // TODO: this connect uses fix ip address
	QFSHelper *fsh = new QFSHelper(); 
    fsh->Connect();
	if (!fsh->IsFileExists(ss_pathname)) {
        LOG4CXX_ERROR(logger_, "Couldn't find snapshot meta: " << vm_id_ << " " << snapshot_id_);
		return false;
	}
	QFSFileHelper *fh = new QFSFileHelper(fsh, ss_pathname, O_RDONLY);
	fh->Open();
	int read_length = fh->GetNextLogSize();
	char *data = new char[read_length];
	fh->Read(data, read_length);
	fh->Close();
	fsh->DisConnect();
    delete fh;
    delete fsh;
    LOG4CXX_INFO(logger_, "Read " << read_length << " from file");

    stringstream buffer;
    buffer.write(data, read_length);
    ssmeta_.Deserialize(buffer);
    LOG4CXX_INFO(logger_, "Snapshot meta loaded: " << ssmeta_.vm_id_ << " " << ssmeta_.snapshot_id_);
    return true;
}

bool SnapshotControl::SaveSnapshotMeta()
{
	QFSHelper fsh;
	fsh.Connect();

	string ss_path = "root/" + vm_id_;
    if (!fsh.IsDirectoryExists(ss_path))
        fsh.CreateDirectory(ss_path);
    string ss_pathname = ss_path + "/" + snapshot_id_;
    if (fsh.IsFileExists(ss_pathname))
        fsh.RemoveFile(ss_pathname);
	QFSFileHelper fh(&fsh, ss_pathname, O_WRONLY);
	fh.Create();

	stringstream buffer;
	ssmeta_.Serialize(buffer);
	LOG4CXX_INFO(logger_, "Snapshot meta size " << buffer.str().size());
    fh.Write((char *)buffer.str().c_str(), buffer.str().size());
    fh.Close();
    return true;
}




















