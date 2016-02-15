#ifndef _NEWCOLUMNTABLEINSERTER_H
#define _NEWCOLUMNTABLEINSERTER_H

#include <trident/binarytables/binarytableinserter.h>

class NewColumnTableInserter: public BinaryTableInserter {
private:
    uint64_t largestElement1, largestElement2, largestGroup;

    std::vector<std::pair<uint64_t, uint64_t>> tmpfirstpairs;
    std::vector<uint64_t> tmpsecondpairs;

public:

    int getType() {
        return NEWCOLUMN_ITR;
    }

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();
};

#endif
