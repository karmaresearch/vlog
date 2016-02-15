#ifndef POSITR_H_
#define POSITR_H_

#include <trident/kb/consts.h>
#include <trident/iterators/pairitr.h>

#include <iostream>

class Querier;

class AggrItr: public PairItr {
    private:
        Querier *q;
        PairItr *mainItr;
        PairItr *secondItr;
        bool noSecColumn;

        bool hasNextChecked, n;
        int idx;

        //long p;

        char strategy(long coordinates) {
            return (char) ((coordinates >> 48) & 0xFF);
        }

        short file(long coordinates) {
            return (short)((coordinates >> 32) & 0xFFFF);
        }

        int pos(long coordinates) {
            return (int) coordinates;
        }

        void setup_second_itr(const int idx);

    public:
        int getTypeItr() {
            return AGGR_ITR;
        }

        long getValue1() {
            return mainItr->getValue1();
        }

        long getValue2() {
            return secondItr->getValue2();
        }

        bool hasNext();

        void next();

        void mark();

        void reset(const char i);

        void clear();

        void gotoFirstTerm(long c1);

        void gotoSecondTerm(long c2);

        void init(int idx, PairItr* itr, Querier *q);

        uint64_t getCardinality();

        uint64_t estCardinality();

        long getCount();

        //uint64_t estimateCardinality();

        void setConstraint1(const long c1) {
            PairItr::setConstraint1(c1);
            mainItr->setConstraint1(c1);
        }

        void setConstraint2(const long c2) {
            PairItr::setConstraint2(c2);
            if (secondItr)
                secondItr->setConstraint2(c2);
        }

        PairItr *getMainItr() {
            return mainItr;
        }

        PairItr *getSecondItr() {
            return secondItr;
        }

        void ignoreSecondColumn() {
            noSecColumn = true;
            if (mainItr != NULL) {
                mainItr->ignoreSecondColumn();
            } else {
                throw 10;
            }
        }
};

#endif
