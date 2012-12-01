#include "append/store.h"
#include "append/append_store.h"
#include <iostream>
#include <vector>

using namespace apsara::AppendStore;
using namespace std;

int main (int argc, char **argv)
{
    if (argc <2)
    {
        cout<<"usage: "<<argv[0]<<" pangu://localcluster/dedup/000_1234567890/11196402326868950822segment/"<<endl;
        exit(0);
    }
    string path = argv[1];

    StoreParameter para;
    para.mPath = path;
    para.mAppend = false;

    std::auto_ptr<Store> store(StoreFactory::Load(para, panguStore));
    Scanner* s = store->GetScanner();

    while (1) 
    {
        std::auto_ptr<string>  h( new string());
        std::auto_ptr<string>  data(new string());
        if (s->Next(h.get(),data.get()) == false)
        {
            break;
        }
        Handle handle(*h);
        cout<<data->size()<<endl;
        cout<<handle.mChunkId<<endl;
        cout<<handle.mIndex<<endl;
    }

    return 0;
}
