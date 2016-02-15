#ifndef _ROWTABLE_H
#define _ROWTABLE_H

#include <trident/binarytables/binarytable.h>

class RowTable: public BinaryTable {
    private:
        int comprValue1;
        int comprValue2;
        int diffValue1;
        long previousValue1;

        long markPreviousValue1;

        long readFirstTerm();
        long readSecondTerm();

        //Used for inserting
        long nElements;
        void writeFirstTerm(long t1);
        void writeSecondTerm(long t2);

    public:
        void next_pair();

        void first();

        void moveToClosestFirstTerm(long c1);

        void moveToClosestSecondTerm(long c1, long c2);

        void mark();

        void reset(const char i);

        void setCompressionMode(int v1, int v2);

        void setDifferenceMode(int d1);

        void ignoreSecondColumn();

        uint64_t getNFirsts();

        uint64_t estNFirsts();

        uint64_t getNSecondsFixedFirst();

        uint64_t estNSecondsFixedFirst();

        long getCount();

        int getType() {
            return ROW_ITR;
        }

        void columnNotIn(uint8_t columnId, BinaryTable *other,
                uint8_t columnOther, SequenceWriter *output) {
            throw 10;
            //not yet supported
        }
};

#endif
