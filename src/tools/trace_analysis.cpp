#include <iostream>
#include <fstream>
#include <string>
#include "../snapshot/trace_types.h"

using namespace std;

void usage(char *progname)
{
	cout << "Usage: " << progname << " logfile [-d]";
}

int main(int argc, char **argv)
{
	if (argc < 2 || argc > 3) {
		usage(argv[0]);
		return 1;
	}

	bool show_detail = false;

	if (argc == 3) {
		show_detail = true;
	}

	ifstream is;
	Block bl;
	uint64_t size = 0, nblocks = 0;
	/*
    uint64_t new_data = 0, l1_data = 0, l2_data = 0, l3_data = 0, l3_cache = 0;
    uint64_t gb = 1024 * 1024 * 1024;
    */

	is.open(argv[1], std::ios_base::in | std::ios_base::binary);
	if (!is.is_open()) {
		cout << "open failed: " << argv[1];
		return 0;
	}

	while(bl.FromStream(is)) {
		size += bl.size_;
		nblocks++;
        /*		
        switch (bl.file_id_) {
        case 0:
            new_data += bl.size_;
            break;
        case IN_PARENT:
            l1_data += bl.size_;
            break;
        case IN_CDS:
            l3_data += bl.size_;
            break;
        case IN_DIRTY_SEG:
            l2_data += bl.size_;
            break;
        case IN_CDS_CACHE:
            l3_data += bl.size_;
            l3_cache += bl.size_;
            break;
        default:
            break;
        }
		*/
		if (show_detail)
			cout << bl.ToString();

		if (bl.size_ == 0 || bl.size_ > 16384) {
			cout << "corrupted block info: " << bl.ToString() << endl;
		}
	}
	cout << argv[1] << ": " <<
        nblocks << " blocks, " << 
        size << " bytes, " <<
        /*
        (float)new_data/gb << "new, " <<
        (float)l1_data/gb << " l1, " <<
        (float)l2_data/gb << " l2, " <<
        (float)l3_data/gb << " l3, "
        (float)l3_cache/gb << " l3_cache" <<
        */
        endl;
	is.close();
	exit(0);
}
