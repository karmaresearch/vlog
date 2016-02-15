#ifndef ARRAYITR_H_
#define ARRAYITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <inttypes.h>
#include <vector>

typedef std::vector<std::pair<uint64_t, uint64_t> > Pairs;

class ArrayItr: public PairItr {
    private:
        Pairs *array;
        int nElements;
        int pos;
        long v1, v2;

        int markPos;
        bool hasNextChecked;
        bool n;
        bool ignSecondColumn;
        long countElems;

        static int binarySearch(Pairs *array, int end, uint64_t key);

    public:
        int getTypeItr() {
            return ARRAY_ITR;
        }

        long getValue1() {
            return v1;
        }

        long getValue2() {
            return v2;
        }

        long getCount();

        uint64_t getCardinality();

        uint64_t estCardinality();

        void ignoreSecondColumn();

        bool hasNext();

        void next();

        void mark();

        void reset(const char i);

        void clear();

        void gotoFirstTerm(long c1);

        void gotoSecondTerm(long c2);

        void init(Pairs* values, int64_t v1, int64_t v2);
};

#endif /* ARRAYITR_H_ */
