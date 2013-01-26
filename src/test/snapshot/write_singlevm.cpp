#include "store.h"
#include "append_store.h"
#include <iostream>
#include <cstdlib>
#include <time.h>
#include "append_store_types.h"
#include "store.h"
#include "data_source.h"

using namespace std;

void getvmIDAndsnapshotID(string path, string &vmid, string &snapid) {

}

string ROOT_DIRECTORY = "SNAPSHOT";

int main(int argc, char *argv[]) {
	PanguAppendStore *pas = NULL;
	StoreParameter sp = StoreParameter(); ;
	std::stringstream stream;
	string snapshotName;
	string storeName;
	string sampleFile;
	string snapshotFile;
	string vmID;
	string snapshotID;

	if(argc < 3) { 
		cout << endl << "enter snapshot file path and sample file path";
		return -1;
	}

	snapshotFile = string(argv[1]);
	sampleFile = string(argv[2]);

	getvmIDAndsnapshotID(snapshotFile, vmID, snapshotID);

	/* Init Append Store */
	stream << ROOT_DIRECTORY << "/" << snapshotName;
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
			string handle = pas->Append(blockMetaIter->data_);
			blockMetaIter->handle_ = 123;
			memcpy(& (blockMetaIter->handle_), &handle, sizeof(blockMetaIter->handle_));
		} 
		// serialize segmentMeta and write
		sstream.str("");
		segmentMeta.Serialize(sstream);
		string h = pas->Append(sstream.str().c_str());
		// update segmentMeta handle
		memcpy(& (segmentMeta.handle_), &h, sizeof(segmentMeta.handle_));
		// add segmentmeta to snapshotMeta
		snapshotMeta.segment_list_.push_back(segmentMeta);
	}

	// write snapshotMeta to file !! 

	pas->Flush();
	pas->Close();
	return 0;
}
