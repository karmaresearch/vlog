#ifndef _TUPLEKB_ITR
#define _TUPLEKB_ITR

#include <trident/iterators/tupleiterators.h>
#include <trident/iterators/pairitr.h>

class VTuple;
class Querier;
class TridentTupleItr : public TupleIterator {
private:
    Querier *querier;
    PairItr *physIterator;
    int idx;
    int *invPerm;

    bool onlyVars;
    uint8_t varsPos[3];
    uint8_t sizeTuple;

    std::vector<std::pair<uint8_t, uint8_t>> equalFields;

    bool nextProcessed;
    bool nextOutcome;
    size_t processedValues;

    bool checkFields();

public:
    TridentTupleItr();

    PairItr *getPhysicalIterator() {
        return physIterator;
    }

    void moveToClosestFirstTerm(const uint64_t t) {
        physIterator->gotoFirstTerm(t);
        nextProcessed = false;
    }

    void moveToClosestSecondTerm(const uint64_t t) {
        physIterator->gotoSecondTerm(t);
        nextProcessed = false;
    }

    void ignoreSecondColumn() {
        physIterator->ignoreSecondColumn();
    }

    void init(Querier *querier, const VTuple *literal,
              const std::vector<uint8_t> *fieldsToSort) {
        init(querier, literal, fieldsToSort, false);
    }

    void init(Querier *querier, const VTuple *literal,
              const std::vector<uint8_t> *fieldsToSort, bool onlyVars);

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);   // No Term_t: overrides method in trident

    ~TridentTupleItr();

    void clear();
};

#endif
