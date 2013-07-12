#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <algorithm>
#include "../../snapshot/trace_types.h"
#include "snapshot.h"

using namespace std;

const size_t MAX_SIMILAR_SEARCH = 10;

struct DedupCounter {
    uint64_t num_bytes_;
    uint64_t num_blocks_;

    DedupCounter() {num_bytes_ = 0; num_blocks_ = 0;}
};

void usage(char* progname)
{
    cout << "Usage: " << progname << " cds_name snapshot_list [prefix] [suffix]" << endl;
    cout << "snapshot_list is a list of vm snapshot trace files." << endl;
    cout << "prefix is used to specify the path to vm snapshot trace files." << endl;
    cout << "suffix is used to specify the trace type, such like .bv4 or .v4" << endl;
}

int main(int argc, char** argv)
{
    if (argc < 3 || argc > 5) {
        usage(argv[0]);
        return 1;
    }

    string cds_name = argv[1];
    string list_fname = argv[2];
    string fprefix, fsuffix;
    if (argc > 3) {
        fprefix = argv[3];
    }
    if (argc > 4) {
        fsuffix = argv[4];
    }

    // prepare CDS
    std::vector<Block> cds;
    ifstream cds_ifs;
    cds_ifs.open(cds_name.c_str(), ios::in | ios::binary);
    Block blk;
    while (blk.FromStream(cds_ifs)) {
        cds.push_back(blk);
    }
    cds_ifs.close();
    sort(cds.begin(), cds.end());
    cout << "CDS file " << cds_name 
         << " is loaded and sorted, it has " 
         << cds.size() << " objects" << endl;

    // open and load all snapshot traces
    vector<Snapshot*> snapshots;
    ifstream ss_ifs;
    ss_ifs.open(list_fname.c_str(), ios::in);
    while (ss_ifs.good()) {
        // open all the snapshots for that VM
        string ss_fname;
	std::getline(ss_ifs, ss_fname);
	if (ss_fname.length() == 0) {
	    continue;
	}
	string trace_fname = fprefix + ss_fname + fsuffix;
	//cout << "loading " << trace_fname << " into memory" << endl;
	snapshots.push_back(new Snapshot(trace_fname));
    }
    ss_ifs.close();
        
    // dedup each snapshot against its parent, count the new data brought by each snapshot
    int num_ss = snapshots.size();
    DedupCounter* new_data = new DedupCounter[num_ss];
    DedupCounter* l1_data = new DedupCounter[num_ss];
    DedupCounter* l2_data = new DedupCounter[num_ss];
    DedupCounter* l3_data = new DedupCounter[num_ss];
    DedupCounter raw, after_l1, after_l2, after_l3;
    cout << "processing " << num_ss << " snapshots in " << list_fname << endl;
    for (int i = 0; i < num_ss; i++) {
	// sort the parent snapshot for lookup
	if (i != 0) {
	    snapshots[i - 1]->Sort();
	}

	size_t seg_id = 0;
	while (true) {
	    // if (seg_id == 0) cout << endl;
	    // cout << " l1: " << l1_data[i].num_blocks_
	    //  << " l2: " << l2_data[i].num_blocks_
	    //  << " l3: " << l3_data[i].num_blocks_
	    //  << " new: " << new_data[i].num_blocks_ << endl;

	    Segment* p_seg = snapshots[i]->GetSegment(seg_id);
	    if (p_seg == NULL) {
		break;
	    }
	    raw.num_blocks_ += p_seg->blocklist_.size();
	    raw.num_bytes_ += p_seg->size_;
	    if (i != 0) {
		//level 1 : check dirty bits
		Segment* p_parent_seg = snapshots[i - 1]->GetSegment(seg_id);
		if (p_parent_seg != NULL && *p_seg == *p_parent_seg) {
		    // detected at level 1
		    seg_id += 1;
		    l1_data[i].num_blocks_ += p_seg->blocklist_.size();
		    l1_data[i].num_bytes_ += p_seg->size_;
		    continue;
		}
		// level 2 : fine-grained local dedup (similar segments, offset-aligned segment)
		// cout << p_seg->GetMinHash().ToString() << endl;
		after_l1.num_blocks_ += p_seg->blocklist_.size();
		after_l1.num_bytes_ += p_seg->size_;
		
		vector<size_t>* p_similar_segs = snapshots[i - 1]->FindSimilarSegments(p_seg->GetMinHash());
		bool search_aligned_segment = true;
		// check if similar segments contains the offset-aligned segment
		if (p_similar_segs != NULL) {
		    //cout << "found " << p_similar_segs->size() << " similar segments" << endl;
		    for (size_t j = 0; j < p_similar_segs->size() && j < MAX_SIMILAR_SEARCH; j ++) {
			if ((*p_similar_segs)[j] == seg_id) {
			    search_aligned_segment = false;
			}
		    }
		}
		// dedup each block in several steps: similar segments, offset-aligned segment, CDS
		for (vector<Block>::iterator it = p_seg->blocklist_.begin(); 
		     it != p_seg->blocklist_.end(); ++ it) {
		    
		    bool found = false;
		    // search similar segments first
		    if (p_similar_segs != NULL) {
			for (size_t j = 0; j < p_similar_segs->size() && j < MAX_SIMILAR_SEARCH; j ++) {
			    if (snapshots[i - 1]->SearchInSegment((*p_similar_segs)[j], *it)) {
				// detected at level 2
				l2_data[i].num_blocks_ += 1;
				l2_data[i].num_bytes_ += it->size_;
				found = true;
				break;
			    }
			}
		    }
		    if (found) {
			continue;
		    }
		    // offset-aligned segment
		    if (search_aligned_segment 
			&& snapshots[i - 1]->SearchInSegment(seg_id, *it)) {
			// detected at level 2
			l2_data[i].num_blocks_ += 1;
			l2_data[i].num_bytes_ += it->size_;
			continue;
		    }
		    // level 3 : search CDS
		    after_l2.num_blocks_ += 1;
		    after_l2.num_bytes_ += it->size_;

		    if (binary_search(cds.begin(), cds.end(), *it)) {
			// detected at level 3
			l3_data[i].num_blocks_ += 1;
			l3_data[i].num_bytes_ += it->size_;
			continue;
		    }
		    // backup new data
		    after_l3.num_blocks_ += 1;
		    after_l3.num_bytes_ += it->size_;
		    new_data[i].num_blocks_ += 1;
		    new_data[i].num_bytes_ += it->size_;
		}
	    }
	    // for the first snapshot, only look CDS
	    else {
		after_l1.num_blocks_ += p_seg->blocklist_.size();
		after_l1.num_bytes_ += p_seg->size_;
		after_l2.num_blocks_ += p_seg->blocklist_.size();
		after_l2.num_bytes_ += p_seg->size_;

		for (vector<Block>::iterator it = p_seg->blocklist_.begin(); 
                     it != p_seg->blocklist_.end(); ++ it) {
		    if (binary_search(cds.begin(), cds.end(), *it)) {
			// detected at level 3
			continue;
		    }
		    // backup new data
		    after_l3.num_blocks_ += 1;
		    after_l3.num_bytes_ += it->size_;
		    new_data[i].num_blocks_ += 1;
		    new_data[i].num_bytes_ += it->size_;
		}
	    }
	    seg_id += 1;
	}
    }
    
    // print and clean up
    for (int j = 0; j < num_ss; j++) {
	cout << "snapshot" << j 
	     << " l1: " << l1_data[j].num_blocks_ << " blocks, " << l1_data[j].num_bytes_ << " bytes,"
	     << " l2: " << l2_data[j].num_blocks_ << " blocks, " << l2_data[j].num_bytes_ << " bytes,"
	     << " l3: " << l3_data[j].num_blocks_ << " blocks, " << l3_data[j].num_bytes_ << " bytes,"
	     << " new: " << new_data[j].num_blocks_ << " blocks, " << new_data[j].num_bytes_ << " bytes" << endl;
    }

    cout << "raw: " << " blocks: " << raw.num_blocks_ << " size: " << raw.num_bytes_ << endl;
    cout << "after l1 dedup: " << " blocks: " << after_l1.num_blocks_ << " size: " << after_l1.num_bytes_ << endl;
    cout << "after l2 dedup: " << " blocks: " << after_l2.num_blocks_ << " size: " << after_l2.num_bytes_ << endl;
    cout << "after l3 dedup: " << " blocks: " << after_l3.num_blocks_ << " size: " << after_l3.num_bytes_ << endl;

    delete[] l1_data;
    delete[] l2_data;
    delete[] l3_data;
    delete[] new_data;
    for (vector<Snapshot*>::iterator it = snapshots.begin(); 
	 it != snapshots.end(); ++it) {
	delete (*it);
    }
    return 0;
}



















