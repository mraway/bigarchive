/*
 * dirty bit map for current snapshot
 */

#ifndef _DIRTY_BIT_H_
#define _DIRTY_BIT_H_

#include <iostream>
#include <string>
#include <vector>
#include <string.h>

using namespace std;

class DirtyBitMap
{
public:
    DirtyBitMap();

    ~DirtyBitMap();

    void Generate(istream& current, istream& parent);

    void ToStream(ostream& os);

    void FromStream(istream& is);

    bool Test(int pos);

private:
    vector<char> bitmap_;
};

#endif /* _DIRTY_BIT_H_ */


