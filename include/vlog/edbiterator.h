#ifndef _EDBITERATOR_H
#define _EDBITERATOR_H

#include <vlog/concepts.h>

class EDBIterator {
public:
    virtual bool hasNext() = 0;

    virtual void next() = 0;

    virtual Term_t getElementAt(const uint8_t p) = 0;

    virtual PredId_t getPredicateID() = 0;

    virtual void moveTo(const uint8_t field, const Term_t t) = 0;

    virtual void skipDuplicatedFirstColumn() = 0;

    virtual void clear() = 0;

    virtual ~EDBIterator() {}
};

#endif
