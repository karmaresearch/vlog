#ifndef _ROWTABLEINSERTER_H
#define _ROWTABLEINSERTER_H

#include <trident/kb/consts.h>
#include <trident/binarytables/binarytableinserter.h>

class RowTableInserter: public BinaryTableInserter {
private:
    int comprValue1;
    int comprValue2;
    int diffValue1;
    long previousValue1;
    long nElements;

    void writeFirstTerm(long t1);
    void writeSecondTerm(long t2);

public:

    int getType() {
        return ROW_ITR;
    }

    void setCompressionMode(int v1, int v2);

    void setDifferenceMode(int d1);

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();

};
#endif
