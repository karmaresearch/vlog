#ifndef _TUPLE_ITR_H
#define _TUPLE_ITR_H

#include <inttypes.h>
#include <stddef.h>
#include <vector>

class TupleIterator {
public:
    virtual bool hasNext() = 0;

    virtual void next() = 0;

    virtual size_t getTupleSize() = 0;

    virtual uint64_t getElementAt(const int pos) = 0;

    virtual ~TupleIterator() {}
};

#endif
