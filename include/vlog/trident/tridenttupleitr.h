#ifndef _TUPLEKB_ITR
#define _TUPLEKB_ITR

#include <trident/iterators/tupleiterators.h>
#include <trident/iterators/pairitr.h>

#include <mutex>

class VTuple;
class Querier;
class TridentTupleItr : public TupleIterator {
private:
    std::mutex *mutex;
    Querier *querier;
    PairItr *physIterator;
    int idx;
    // int *invPerm;

    // bool onlyVars;
    uint8_t varsPos[3];
    uint8_t sizeTuple;
    int nvars;

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

    void move(const uint64_t t1, const uint64_t t2) {
        physIterator->moveto(t1, t2);
        nextProcessed = false;
    }

    void ignoreSecondColumn() {
        physIterator->ignoreSecondColumn();
    }

    void init(Querier *querier, const VTuple *literal,
              const std::vector<uint8_t> *fieldsToSort, std::mutex *mutex) {
        init(querier, literal, fieldsToSort, false, mutex);
    }

    void init(Querier *querier, const VTuple *literal,
              const std::vector<uint8_t> *fieldsToSort, bool onlyVars, std::mutex *mutex);

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);   // No Term_t: overrides method in trident

    const char* getUnderlyingArray(uint8_t column);

    size_t getCardinality();

    std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray(uint8_t column);

    ~TridentTupleItr();

    void clear();
};

#endif
