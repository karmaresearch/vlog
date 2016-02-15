#ifndef _EDBKBITERATOR_H
#define _EDBKBITERATOR_H

#include <vlog/trident/tridenttupleitr.h>
#include <vlog/edbiterator.h>
#include <vlog/concepts.h>
#include <vlog/consts.h>

class TridentIterator : public EDBIterator {
private:
    uint8_t nfields;

    TridentTupleItr kbItr;
    PredId_t predid;

    //long nconcepts;

public:
    TridentIterator() {
        //nconcepts = 0;
    }

    void init(PredId_t predid, Querier *q, const Literal &literal);

    void init(PredId_t predid, Querier *q, const Literal &literal,
              const std::vector<uint8_t> &fields);

    bool hasNext();

    void next();

    void clear();

    void skipDuplicatedFirstColumn();

    void moveTo(const uint8_t fieldId, const Term_t t);

    PredId_t getPredicateID() {
        return predid;
    }

    Term_t getElementAt(const uint8_t p);

    ~TridentIterator() {
        //BOOST_LOG_TRIVIAL(debug) << "Iterated over " << nconcepts;
    }
};

#endif
