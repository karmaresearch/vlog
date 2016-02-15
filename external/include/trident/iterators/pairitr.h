#ifndef PAIRITR_H_
#define PAIRITR_H_


#include <inttypes.h>

#define NO_CONSTRAINT -1

class PairItr {
protected:
    long constraint1;
    long constraint2;
    long key;
public:
    virtual int getTypeItr() = 0;

    virtual long getValue1() = 0;

    virtual long getValue2() = 0;

    long getKey() {
        return key;
    }

    void setKey(long key) {
        this->key = key;
    }

    virtual bool hasNext() = 0;

    virtual void next() = 0;

    virtual void ignoreSecondColumn() = 0;

    virtual long getCount() = 0;

    virtual uint64_t getCardinality() = 0;

    virtual uint64_t estCardinality() = 0;

    virtual ~PairItr() {
    }

    virtual void clear() = 0;

    long getConstraint2() {
        return constraint2;
    }

    virtual void setConstraint2(const long c2) {
        constraint2 = c2;
    }

    long getConstraint1() {
        return constraint1;
    }

    virtual void setConstraint1(const long c1) {
        constraint1 = c1;
    }

    virtual void mark() = 0;

    virtual void reset(const char i) = 0;

    virtual void gotoKey(long k) {
        throw 10; //The only iterator that can use this method is scanitr,
        //whic overrides it.
    }

    virtual void gotoFirstTerm(long c1) = 0;

    virtual void gotoSecondTerm(long c2) = 0;
};

#endif
