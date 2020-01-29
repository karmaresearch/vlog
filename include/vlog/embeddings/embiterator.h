#ifndef _EMB_ITR_H
#define _EMB_ITR_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>
#include <vlog/segment.h>

class EmbIterator : public EDBIterator {
    private:
        PredId_t predid;
        uint64_t begTerm, endTerm, nextTerm;
        uint64_t range;

        bool usePointers;
        Term_t *start;

    public:
        EmbIterator(PredId_t predid,
                uint64_t begTerm,
                uint64_t endTerm,
                uint64_t range) :
            predid(predid), begTerm(begTerm),
            endTerm(endTerm), nextTerm(begTerm),
            range(range), usePointers(false), start(NULL) {
            }

        EmbIterator(PredId_t predid,
                uint64_t begTerm,
                uint64_t endTerm,
                uint64_t range,
                Term_t *start) : predid(predid), begTerm(begTerm),
        endTerm(endTerm), nextTerm(begTerm), range(range),
        usePointers(true), start(start) {
        }

        bool hasNext() {
            return nextTerm < endTerm;
        }

        void next() {
            if (nextTerm < endTerm)
                nextTerm++;
        }

        Term_t getElementAt(const uint8_t p) {
            if (p == 0) {
                if (usePointers) {
                    auto diff = nextTerm - 1 - begTerm;
                    return *(start + diff);
                } else {
                    return nextTerm - 1;
                }
            } else {
                return nextTerm - 1 + range;
            }
        }

        PredId_t getPredicateID() {
            return predid;
        }

        void skipDuplicatedFirstColumn() {
        }

        void clear() {
            nextTerm = begTerm;
        }
};

#endif
