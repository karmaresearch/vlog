#ifndef _TUPLEKBITERATOR_H
#define _TUPLEKBITERATOR_H

#include <trident/iterators/tupleiterators.h>
#include <trident/iterators/pairitr.h>
#include <vector>

class Tuple;
class Querier;
class TupleKBItr : public TupleIterator {
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
    TupleKBItr();

    PairItr *getPhysicalIterator() {
        return physIterator;
    }

    void ignoreSecondColumn() {
        physIterator->ignoreSecondColumn();
    }

    void init(Querier *querier, const Tuple *literal,
              const std::vector<uint8_t> *fieldsToSort) {
        init(querier, literal, fieldsToSort, false);
    }

    void init(Querier *querier, const Tuple *literal,
              const std::vector<uint8_t> *fieldsToSort, bool onlyVars);

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);

    ~TupleKBItr();

    void clear();
};

#endif
