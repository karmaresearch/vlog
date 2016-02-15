#ifndef _COLUMNTABLEINSERTER_H
#define _COLUMNTABLEINSERTER_H

#include <trident/binarytables/binarytableinserter.h>

class ColumnTableInserter: public BinaryTableInserter {
private:
    uint64_t largestElement, maxGroupSize;
    std::vector<std::pair<uint64_t, uint64_t>> tmpfirstpairs;
    std::vector<uint64_t> tmpsecondpairs;
    int compr1;
    int compr2;
    uint8_t bytesPerFirstEntry, bytesPerPointer, bytesPerNElements;

    void writeFirstTerm(long t1);

    void writeSecondTerm(long t2);

    uint8_t getNBytes(const int comprType, const long value) const;

public:

    int getType() {
        return COLUMN_ITR;
    }

    void appendBlock();

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();

    void setCompressionMode(int v1, int v2);
};

#endif
