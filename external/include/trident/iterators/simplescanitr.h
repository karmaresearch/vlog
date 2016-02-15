#ifndef SIMPLECANITR_H_
#define SIMPLECANITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/tree/coordinates.h>
#include <trident/kb/consts.h>

#include <tridentcompr/utils/lz4io.h>

class Querier;

class SimpleScanItr: public PairItr {

private:
    Querier *q;
    long v1, v2;
    LZ4Reader *reader;

public:
    void init(Querier *q);

    int getTypeItr() {
        return SIMPLESCAN_ITR;
    }

    long getValue1() {
        return v1;
    }

    long getValue2() {
        return v2;
    }

    bool allowMerge() {
        return true;
    }

    void mark();

    long getCount() {
        throw 10; //not supported
    }

    void ignoreSecondColumn() {
        throw 10; //not supported
    }

    void reset(const char i);

    bool hasNext();

    void next();

    void clear();

    uint64_t getCardinality();

    //uint64_t estimateCardinality();

    void gotoFirstTerm(long c1);

    void gotoSecondTerm(long c2);

};

#endif
