#include "snapshot_control.h"

const string kBasePath = "root";

LoggerPtr SnapshotControl::logger_ = Logger::getLogger("SnapshotControl");

SnapshotControl::SnapshotControl(const string& trace_file)
{
    trace_file_ = trace_file;
    ParseTraceFile();
    Init();
}


SnapshotControl::SnapshotControl(const string& os_type, const string& disk_type, const string& vm_id, const string& ss_id)
{
    os_type_ = os_type;
    disk_type_ = disk_type;
    ss_meta_.vm_id_ = vm_id;
    ss_meta_.snapshot_id_ = ss_id;
    Init();
}

void SnapshotControl::Init()
{
    vm_path_ = "/" + kBasePath + "/" + ss_meta_.vm_id_;
    vm_meta_pathname_ = vm_path_ + "/vm.meta";
    store_path_ = vm_path_ + "/" + "appendstore";
    ss_meta_pathname_ = vm_path_ + "/" + ss_meta_.snapshot_id_ + ".meta";
    primary_filter_pathname_ = vm_path_ + "/" + ss_meta_.snapshot_id_ + ".bm1";
    secondary_filter_pathname_ = vm_path_ + "/" + ss_meta_.snapshot_id_ + ".bm2";
    seg_pos_ = 0;
    ss_meta_.size_ = 0;
}

void SnapshotControl::SetAppendStore(PanguAppendStore* pas)
{
    store_ptr_ = pas;
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
    // open the snapshot meta file in qfs and read it
    // TODO: currently the read/write apis provide a log style file
	QFSHelper fsh; 
    fsh.Connect();
	if (!fsh.IsFileExists(ss_meta_pathname_)) {
        LOG4CXX_ERROR(logger_, "Couldn't find snapshot meta: " << ss_meta_.vm_id_ << " " << ss_meta_.snapshot_id_);
		return false;
	}
	QFSFileHelper fh(&fsh, ss_meta_pathname_, O_RDONLY);
	fh.Open();
	int read_length = fh.GetNextLogSize();
	char *data = new char[read_length];
	fh.Read(data, read_length);
    LOG4CXX_INFO(logger_, "Read " << read_length << " from file");

    stringstream buffer;
    buffer.write(data, read_length);
    ss_meta_.Deserialize(buffer);
    ss_meta_.DeserializeRecipe(buffer);
    LOG4CXX_INFO(logger_, "Snapshot meta loaded: " << ss_meta_.vm_id_ << " " << ss_meta_.snapshot_id_);

	fh.Close();
	fsh.DisConnect();
    delete[] data;
    return true;
}

bool SnapshotControl::SaveSnapshotMeta()
{
	QFSHelper fsh;
	fsh.Connect();

    if (!fsh.IsDirectoryExists(vm_path_))
        fsh.CreateDirectory(vm_path_);
    if (fsh.IsFileExists(ss_meta_pathname_))
        fsh.RemoveFile(ss_meta_pathname_);
	QFSFileHelper fh(&fsh, ss_meta_pathname_, O_WRONLY);
	fh.Create();

	stringstream buffer;
	ss_meta_.Serialize(buffer);
    ss_meta_.SerializeRecipe(buffer);
	LOG4CXX_INFO(logger_, "Snapshot meta size " << buffer.str().size());
    fh.Write((char *)buffer.str().c_str(), buffer.str().size());

    fh.Close();
    fsh.DisConnect();
    return true;
}

bool SnapshotControl::LoadSegmentRecipe(SegmentMeta& sm)
{
    if (seg_pos_ >= ssmeta_.snapshot_recipe_.size())
        return false;
    sm = ssmeta_.snapshot_recipe_[seg_pos_++];
    string data;
    string handle((char*)&sm.handle_, sizeof(sm.handle_));
    store_ptr_->Read(handle, &data);
    LOG4CXX_INFO(logger_, "Read segment meta : " << data.size());
    
    stringstream ss(data);
    sm.DeserializeRecipe(ss);
    return true;
}

bool SnapshotControl::SaveSegmentRecipe(SegmentMeta& sm)
{
    stringstream buffer;
    sm.SerializeRecipe(buffer);
    string handle = store_ptr_->Append(buffer.str());
    sm.SetHandle(handle);
    return true;
}

bool SnapshotControl::SaveBlockData(BlockMeta& bm)
{
    string data(bm.data_, bm.size_);
    string handle = store_ptr_->Append(data);
    bm.SetHandle(handle);
    bm.flags_ |= IN_AS;
    return true;
}

bool SnapshotControl::InitBloomFilters(uint64_t snapshot_size)
{
	QFSHelper fsh;
	fsh.Connect();
	if (!fsh.IsFileExists(vm_meta_pathname_)) {
        if (!fsh.IsDirectoryExists(vm_path_))
            fsh.CreateDirectory(vm_path_);
        // init bloom filter params and store into vm meta
        LOG4CXX_INFO(logger_, "VM meta not found, will create " << vm_meta_pathname_);
        vm_meta_.filter_num_items_ = snapshot_size / AVG_BLOCK_SIZE;
        vm_meta_.filter_num_funcs_ = BLOOM_FILTER_NUM_FUNCS;
        vm_meta_.filter_fp_rate_ = BLOOM_FILTER_FP_RATE;

        QFSFileHelper fh(&fsh, vm_meta_pathname_, O_WRONLY);
        fh.Create();
        stringstream buffer;
        vm_meta_.Serialize(buffer);
        LOG4CXX_INFO(logger_, "VM meta size " << buffer.str().size());
        fh.WriteData((char *)buffer.str().c_str(), buffer.str().size());
        fh.Close();
	}
    else {
        // read bloom filter params
        QFSFileHelper fh(&fsh, vm_meta_pathname_, O_RDONLY);
        fh.Open();
        long read_length = fsh.GetSize(vm_meta_pathname_);
        char *data = new char[read_length];
        fh.Read(data, read_length);
        LOG4CXX_INFO(logger_, "Read " << read_length << " from file");

        stringstream buffer;
        buffer.write(data, read_length);
        vm_meta_.Deserialize(buffer);
        LOG4CXX_INFO(logger_, "VM meta loaded: " 
                     << vm_meta_.filter_num_items_ << " " 
                     << vm_meta_.filter_num_funcs_ << ""
                     << vm_meta_.filter_fp_rate_);
        fh.Close();
        delete[] data;
    }
	fsh.DisConnect();

    // params ready, now init bloom filters
    primary_filter_ptr_ = new BloomFilter<Checksum>(vm_meta_.filter_num_items_, 
                                                   vm_meta_.filter_fp_rate_, 
                                                   kBloomFilterFunctions, 
                                                   vm_meta_.filter_num_funcs_);
    // for fine-grained deletion we need a bigger filter, using different group of hash functions
    secondary_filter_ptr_ = new BloomFilter<Checksum>(vm_meta_.filter_num_items_ * 2, 
                                                   vm_meta_.filter_fp_rate_, 
                                                   &kBloomFilterFunctions[8], 
                                                   vm_meta_.filter_num_funcs_);
    return true;
}

void SnapshotControl::UpdateBloomFilters(const SegmentMeta& sm)
{
    for (size_t i = 0; i < sm.segment_recipe_.size(); i++) {
        primary_filter_ptr_->AddElement(sm.segment_recipe_[i].cksum_);
        secondary_filter_ptr_->AddElement(sm.segment_recipe_[i].cksum_);
    }
}

bool SnapshotControl::SaveBloomFilters()
{
    return SaveBloomFilter(primary_filter_ptr_, primary_filter_pathname_) &&
        SaveBloomFilter(secondary_filter_ptr_, secondary_filter_pathname_);
}

bool SnapshotControl::SaveBloomFilter(BloomFilter<Checksum>* pbf, const string& bf_name)
{
	QFSHelper fsh;
	fsh.Connect();

    if (!fsh.IsDirectoryExists(vm_path_))
        fsh.CreateDirectory(vm_path_);
    if (fsh.IsFileExists(bf_name))
        fsh.RemoveFile(bf_name);

	QFSFileHelper fh(&fsh, bf_name, O_WRONLY);
	fh.Create();

	stringstream buffer;
	pbf ->Serialize(buffer);
	LOG4CXX_INFO(logger_, "Bloom filter primary size " << buffer.str().size());
    fh.Write((char *)buffer.str().c_str(), buffer.str().size());

    fh.Close();
    fsh.DisConnect();
    return true;
}

bool SnapshotControl::RemoveBloomFilters()
{
    return RemoveBloomFilter(primary_filter_pathname_) &&
        RemoveBloomFilter(secondary_filter_pathname_);
}

bool SnapshotControl::RemoveBloomFilter(const string& bf_name)
{
    QFSHelper fsh;
    fsh.Connect();
    if (fsh.IsFileExists(bf_name))
        fsh.RemoveFile(bf_name);
    fsh.DisConnect();
    return true;
}

bool SnapshotControl::LoadBloomFilter(BloomFilter<Checksum>* pbf, const string& bf_name)
{
	QFSHelper fsh; 
    fsh.Connect();
	if (!fsh.IsFileExists(bf_name)) {
        LOG4CXX_ERROR(logger_, "Couldn't find bloom filter: " << bf_name);
		return false;
	}
	QFSFileHelper fh(&fsh, bf_name, O_RDONLY);
	fh.Open();
	int read_length = fh.GetNextLogSize();
	char *data = new char[read_length];
	fh.Read(data, read_length);
    LOG4CXX_INFO(logger_, "Read " << read_length << " from file " << bf_name);

    stringstream buffer;
    buffer.write(data, read_length);
    pbf->Deserialize(buffer);
    LOG4CXX_INFO(logger_, "Bloom filter loaded: " << bf_name);

	fh.Close();
	fsh.DisConnect();
    delete[] data;
    return true;
}











