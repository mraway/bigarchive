#include "store.h"
#include "append_store.h"
#include <iostream>
#include <cstdlib>
#include <time.h>
#include "append_store_types.h"
#include "store.h"
#include "data_source.h"
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
	string handle;
	Timer timer = Timer();
	Timer fullTimer = Timer();
	Timer blkTimer = Timer();
	fullTimer.start();
	int blkCnt = 0;

	if(argc < 3) { 
		cout << endl << "enter snapshot file path and sample file path";
		return -1;
	}

	snapshotFile = string(argv[1]);
	sampleFile = string(argv[2]);

	vmFullName = snapshotFile.substr(snapshotFile.find_last_of('/') + 1);
	if(vmFullName != "") {
		// VM-2819056F.1000-19179-1739-full.vhd.bv4
		type = vmFullName.substr(vmFullName.find_last_of('.') + 1);
		vmName = vmFullName.substr(0, 11);
		vmID = vmName;
		snapshotID = vmFullName.substr(12, vmFullName.find_first_of("-full", 23) - 12);
	}

	cout << endl << "Snapshot file -------- " << snapshotFile;
	cout << endl << "Sample file ---------- " << sampleFile;
	cout << endl << "Snapshot Name -------- " << vmName;
	cout << endl << "vm ID ---------------- " << vmID;
	cout << endl << "snapshot ID ---------- " << snapshotID;
	cout << endl << "vmtype --------------- " << type;

	// exit(-1);

	/* Init Append Store */
	timer.start();
	stream.str("");
	stream << ROOT_DIRECTORY << "/" << vmName << "/" << "append";
	storeName = stream.str();
	sp.mPath = storeName;
	sp.mAppend = true;
	pas = new PanguAppendStore(sp, true);
	cout << endl << "For Append Store creation : " << timer.stop() << " ms";
	// exit(-1);
	/* DataStore and snapshot types */
	
	DataSource ds(snapshotFile, sampleFile);	
	cout << endl << "Data Source created";
	SnapshotMeta snapshotMeta;
	snapshotMeta.vm_id_ = vmID;
	snapshotMeta.snapshot_id_ = snapshotID;
	SegmentMeta segmentMeta;
	BlockMeta blockMeta;
	vector<BlockMeta> blockMetaVec;
	stringstream sstream;
	cout << endl << "reading segments started ";

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
	QFSHelper *fsh = new QFSHelper();
	fsh->Connect();
	stream.str("");
	stream << "ROOT_DIRECTORY" << "/" << vmName;
	fsh->CreateDirectory(stream.str());
	QFSFileHelper *fh = new QFSFileHelper(fsh, stream.str() + "/" + snapshotID, O_WRONLY);
	sstream.str("");
	snapshotMeta.Serialize(sstream);
	fh->Create();
	fh->Write((char *)sstream.str().c_str(), sstream.str().size());
	fh->Close();
	pas->Flush();
	pas->Close();
	cout << endl << "snapshot meta wrote into file " << timer.stop() << " ms";
	cout << endl << "overall time taken " << fullTimer.stop() << " ms";
	return 0;
}

