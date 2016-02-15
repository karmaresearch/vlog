#ifndef _TERMITR_H_
#define _TERMITR_H_

#include <trident/binarytables/tableshandler.h>
#include <trident/iterators/pairitr.h>
#include <trident/tree/coordinates.h>
#include <trident/kb/consts.h>

class TreeItr;

class TermItr: public PairItr {

    private:
        TableStorage *tables;
        size_t currentfile, nfiles;
        int currentMark;
        const char *buffer;
        const char *endbuffer;
        uint64_t size;

    public:
        void init(TableStorage *tables, uint64_t size);

        int getTypeItr() {
            return TERM_ITR;
        }

        short getCurrentFile() {
            return currentfile;
        }

        int getCurrentMark() {
            return currentMark;
        }

        char getCurrentStrat();

        long getValue1() {
            return 0;
        }

        long getValue2() {
            return 0;
        }

        void ignoreSecondColumn() {
            throw 10; //not supported
        }

        long getCount() {
            throw 10; //not supported
        }

        bool hasNext();

        void next();

        void clear();

        uint64_t getCardinality();

        uint64_t estCardinality();

        void mark();

        void reset(const char i);

        void gotoKey(long c);

        void gotoFirstTerm(long c1);

        void gotoSecondTerm(long c2);
};

#endif
