#include "store.h"
#include "../../append-store/append_store.h"
#include <iostream>
#include <cstdlib>
#include <time.h>
#include "../../append-store/append_store_types.h"
#include "store.h"
#include "../../snapshot/data_source.h"
#include "timer.h"

using namespace std;

string ROOT_DIRECTORY = "root";

int main(int argc, char *argv[]) {
	
	PanguAppendStore *pas = NULL;
	StoreParameter sp = StoreParameter(); ;
	std::stringstream stream;
	string vmName, vmFullName;
	string storeName;
	string sampleFile;
	string snapshotFile;
	string vmID;
	string snapshotID;
	string vmName_p1, vmName_p2, type;
	Timer timer = Timer();
	Timer fullTimer = Timer();
	Timer blkTimer = Timer();
	fullTimer.start();
	SnapshotMeta snapshotMeta;
	long blkCnt = 0;
	SegmentMeta segmentMeta;
	BlockMeta blockMeta;
	vector<BlockMeta> blockMetaVec;
	stringstream sstream;
	
	if(argc < 2) { 
		cout << endl << "enter snapshot file path";
		return -1;
	}

	snapshotFile = string(argv[1]);

	vmFullName = snapshotFile.substr(snapshotFile.find_last_of('/') + 1);
	if(vmFullName != "") {
		// VM-2819056F.1000-19179-1739-full.vhd.bv4
		type = vmFullName.substr(vmFullName.find_last_of('.') + 1);
		vmName = vmFullName.substr(0, 11);
		vmID = vmName;
		snapshotID = vmFullName.substr(12, vmFullName.find_first_of("-full", 23) - 12);
	}

	cout << endl << "Snapshot file -------- " << snapshotFile;
	cout << endl << "Snapshot Name -------- " << vmName;
	cout << endl << "vm ID ---------------- " << vmID;
	cout << endl << "snapshot ID ---------- " << snapshotID;
	cout << endl << "vmtype --------------- " << type;

	// exit(-1);

	stream.str("");
	stream << "root" << "/" << vmName << "/" << snapshotID;
	cout << endl << "Reading file : " << stream.str();
	QFSHelper *fsh = new QFSHelper(); fsh->Connect();
	if(! fsh->IsFileExists(stream.str())) {
		cout << endl << "File not found " << endl;
		return 0;
	}
	QFSFileHelper *fh = new QFSFileHelper(fsh, stream.str(), O_RDONLY);
	fh->Open();
	int read_length = fh->GetNextLogSize();
	char *data = new char[read_length];
	fh->Read(data, read_length);
	fh->Close();
	fsh->DisConnect();
	cout << endl << "Read " << read_length << " from file";
	// data[read_length] = '\0';
	sstream.str("");
	sstream.write(data, read_length);
	
	snapshotMeta.Deserialize(sstream);

	cout << endl << "snapshotMeta : " << snapshotMeta.vm_id_;
	cout << endl << "snapshotMeta : " << snapshotMeta.snapshot_id_;

	
	stream.str("");
	stream << "root" << "/" << vmName << "/" << "append";
	sp.mPath = stream.str();
	cout << endl << "Append store location : " << sp.mPath;
	sp.mAppend = false;
	pas = new PanguAppendStore(sp, false);

	string block_data;
	string h;
	Handle handle;
        
        cout << endl << "Reading block data";	
	cout << endl << "Number of segments " << snapshotMeta.segment_list_.size();
	
	for(size_t i = 0; i < snapshotMeta.segment_list_.size(); i++) {
		timer.start();
		// blkCnt++;
		segmentMeta = snapshotMeta.segment_list_[i];
		//memcpy(&h, &segmentMeta.handle_, sizeof(segmentMeta.handle_));
		handle.mChunkId = ((segmentMeta.handle_ >> 48) & 0xffff);
		handle.mIndex = (segmentMeta.handle_ & 0x0000ffffffffffffllu);
		pas->Read(handle.ToString(), &block_data);
		cout << endl << handle.mChunkId << "," << handle.mIndex;
		// cout << endl << ((segmentMeta.handle_ >> 48) & 0xffff) << "," << (segmentMeta.handle_ & 0x0000ffffffffffffllu);
		for(size_t j = 0; j < segmentMeta.block_list_.size(); j++) {
			blockMeta = segmentMeta.block_list_[j];
			handle.mChunkId = ((blockMeta.handle_ >> 48) & 0xffff);
			handle.mIndex = (blockMeta.handle_ & 0x0000ffffffffffffllu);
			pas->Read(handle.ToString(), &block_data);
			/*
 			if(blkCnt % 1000 == 0)
			{
				cout << endl << handle.mChunkId << "," << handle.mIndex;
			}
			*/
		}
		cout << endl << "segment completed, " << (i+1) << ":" << timer.stop() << " ms";
	}
	
	cout << endl << "reading data done : Time : " << fullTimer.stop() << " ms";
	pas->Flush();
	pas->Close();
}

/*

	// Init Append Store 
	timer.start();
	stream.str("");
	stream << ROOT_DIRECTORY << "/" << vmName << "/" << "append";
	storeName = stream.str();
	cout << endl << "For Append Store creation : " << timer.stop() << " ms";
	// exit(-1);
	// DataStore and snapshot types 
	
	DataSource ds(snapshotFile, sampleFile);	
	cout << endl << "Data Source created";

	while(true) {
		//timer.start();
		bool gs = ds.GetSegment(segmentMeta);
		if(!gs) break;
		for(size_t i=0; i < segmentMeta.block_list_.size(); i++) {
			if(blkCnt == 0) blkTimer.start();
			string new_string(segmentMeta.block_list_[i].data_, segmentMeta.GetBlockSize(i)); 
			handle = pas->Append(new_string);
			memcpy(& (segmentMeta.block_list_[i].handle_), &handle, 
					sizeof(segmentMeta.block_list_[i].handle_));	
			blkCnt++;
			if(blkCnt == 10000) { 
				cout << endl << "Wrote 10000 Blocks " << blkTimer.stop() << " ms";
				blkCnt = 0;
			}	
		}
		// serialize segmentMeta and write
		sstream.str("");
		segmentMeta.Serialize(sstream);
		handle = pas->Append(sstream.str().c_str());
		// update segmentMeta handle
		memcpy(& (segmentMeta.handle_), &handle, sizeof(segmentMeta.handle_));
		// add segmentmeta to snapshotMeta
		snapshotMeta.AddSegment(segmentMeta);
	}

	
	// write snapshotMeta to file !! 
	timer.start();
	stream.str("");
	stream << ROOT_DIRECTORY << "/" << vmName;
	fsh->CreateDirectory(stream.str());
	cout << endl << "snapshot meta wrote into file " << timer.stop() << " ms";
	cout << endl << "overall time taken " << fullTimer.stop() << " ms";
	return 0;
}

*/
