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
	timer.start();

	if(argc < 3) { 
		cout << endl << "enter snapshot file path and sample file path";
		return -1;
	}

	snapshotFile = string(argv[1]);
	sampleFile = string(argv[2]);

	vmFullName = snapshotFile.substr(snapshotFile.find_last_of('/') + 1);
	if(vmFullName != "") {
		type = vmFullName.substr(vmFullName.find_last_of('.') + 1);
		vmName = vmFullName.substr(0, 22);
		vmID = vmName;
		snapshotID = vmFullName.substr(23, vmFullName.find_first_of('-', 23) - 23);
	}

	cout << endl << "Snapshot file -------- " << snapshotFile;
	cout << endl << "Sample file ---------- " << sampleFile;
	cout << endl << "Snapshot Name -------- " << vmName;
	cout << endl << "vm ID ---------------- " << vmID;
	cout << endl << "snapshot ID ---------- " << snapshotID;
	cout << endl << "vmtype --------------- " << type;
 	double d = timer.stop();
	cout << d << " milliseconds";
	exit(-1);

	/* Init Append Store */
	stream.str("");
	stream << ROOT_DIRECTORY << "/" << vmName << "/" << "append";
	storeName = stream.str();
	sp.mPath = storeName;
	sp.mAppend = true;
	pas = new PanguAppendStore(sp, true);

	/* DataStore and snapshot types */
	DataSource ds(snapshotFile, sampleFile);	

	SnapshotMeta snapshotMeta;
	snapshotMeta.vm_id_ = vmID;
	snapshotMeta.snapshot_id_ = snapshotID;
	SegmentMeta segmentMeta;
	BlockMeta blockMeta;
	vector<BlockMeta> blockMetaVec;
	stringstream sstream;

	while(true) {
		segmentMeta = SegmentMeta();
		bool gs = ds.GetSegment(segmentMeta);
		if(!gs) break;
		blockMetaVec = segmentMeta.block_list_;
		vector<BlockMeta>::iterator blockMetaIter = blockMetaVec.begin();
		while( blockMetaIter != blockMetaVec.end()) {
			handle = pas->Append(blockMetaIter->data_);
			// do we need to set the data null ???
			memcpy(& (blockMetaIter->handle_), &handle, sizeof(blockMetaIter->handle_));
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
	QFSHelper *fsh = new QFSHelper();
	fsh->Connect();
	stream.str("");
	stream << "ROOT_DIRECTORY" << "/" << vmName;
	fsh->CreateDirectory(stream.str());
	QFSFileHelper *fh = new QFSFileHelper(fsh, stream.str() + "/" + snapshotID, O_WRONLY);
	sstream.str("");
	snapshotMeta.Serialize(sstream);
	fh->Write((char *)sstream.str().c_str(), sstream.str().size());
	fh->Close();
	pas->Flush();
	pas->Close();
	return 0;
}

