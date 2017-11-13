#ifndef _EDBKBITERATOR_H
#define _EDBKBITERATOR_H

#include <vlog/trident/tridenttupleitr.h>
#include <vlog/edbiterator.h>
#include <vlog/concepts.h>
#include <vlog/consts.h>

#include <mutex>

class TridentIterator : public EDBIterator {
    private:
        uint8_t nfields;

        TridentTupleItr kbItr;
        PredId_t predid;
        bool duplicatedFirstColumn;

        //long nconcepts;

    public:
        TridentIterator() {
            //nconcepts = 0;
        }

        void init(PredId_t predid, Querier *q, const Literal &literal, std::mutex *mutex);

        void init(PredId_t predid, Querier *q, const Literal &literal,
                const std::vector<uint8_t> &fields, std::mutex *mutex);

        bool hasNext();

        void next();

        void clear();

        void skipDuplicatedFirstColumn();

        void moveTo(const uint8_t field, const Term_t t);

        PredId_t getPredicateID() {
            return predid;
        }

        bool isDuplicatedColumn() {
            return duplicatedFirstColumn;
        }

        size_t getCardinality() {
            return kbItr.getCardinality();
        }

        const char* getUnderlyingArray(uint8_t column);
        std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray(uint8_t column);

        Term_t getElementAt(const uint8_t p);

        ~TridentIterator() {
        }
};

#endif
