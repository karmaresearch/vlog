#ifndef SCANITR_H_
#define SCANITR_H_

#include <trident/kb/consts.h>
#include <trident/iterators/pairitr.h>
#include <trident/tree/coordinates.h>
#include <trident/iterators/termitr.h>
#include <trident/binarytables/storagestrat.h>
#include <trident/binarytables/tableshandler.h>

class Querier;
class TreeItr;

class ScanItr: public PairItr {

private:
    Querier *q;
    int idx;
    PairItr *currentTable;
    PairItr *reversedItr;
    bool ignseccolumn;

    TermItr *itr1;
    TermItr *itr2;
    TableStorage *storage;
    StorageStrat *strat;

public:
    void init(int idx, Querier *q);

    int getTypeItr() {
        return SCAN_ITR;
    }

    long getValue1() {
        if (!reversedItr)
            return currentTable->getValue1();
        else
            return reversedItr->getValue1();
    }

    long getValue2() {
        if (!reversedItr)
            return currentTable->getValue2();
        else
            return reversedItr->getValue2();
    }

    bool allowMerge() {
        return true;
    }

    void ignoreSecondColumn() {
        ignseccolumn = true;
    }

    long getCount() {
        if (!reversedItr)
            return currentTable->getCount();
        else
            return reversedItr->getCount();
    }

    void mark();

    void reset(const char i);

    bool hasNext();

    void next();

    void clear();

    uint64_t getCardinality();

    uint64_t estCardinality();

    void gotoKey(long k);

    void gotoFirstTerm(long c1);

    void gotoSecondTerm(long c2);
};

#endif
