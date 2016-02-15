#include <trident/iterators/aggritr.h>
#include <trident/kb/querier.h>
#include <iostream>

using namespace std;

void AggrItr::setup_second_itr(const int idx) {
    long coordinates = mainItr->getValue2();
    int idx2;
    if (idx == IDX_POS)
        idx2 = IDX_OPS;
    else if (idx == IDX_PSO)
        idx2 = IDX_SPO;
    else
        throw 10; // not supported
    secondItr = q->getFilePairIterator(idx2, key, strategy(coordinates),
                                       file(coordinates), pos(coordinates));
//    secondItr->mark();
}

bool AggrItr::hasNext() {
    if (!hasNextChecked) {
        if (secondItr == NULL) {
            if (mainItr->hasNext()) {
                mainItr->next();
                if (!noSecColumn) {
                    setup_second_itr(idx);
                    n = secondItr->hasNext();
                    secondItr->next();
                } else {
                    n = true;
                }
            } else {
                n = false;
            }
        } else {
            n = secondItr->hasNext();
            if (n) {
                secondItr->next();
            } else if (constraint1 == NO_CONSTRAINT) {
                q->releaseItr(secondItr);
                secondItr = NULL;
                if (mainItr->hasNext()) {
                    mainItr->next();
                    setup_second_itr(idx);
                    n = secondItr->hasNext();
                    if (n) {
                        secondItr->next();
                    }
                } else {
                    n = false;
                }
            }
        }
        hasNextChecked = true;
    }
    return n;
}

long AggrItr::getCount() {
    assert(noSecColumn);
    setup_second_itr(idx);
    long els = secondItr->getCardinality();
    q->releaseItr(secondItr);
    secondItr = NULL;
    return els;
}

void AggrItr::next() {
    hasNextChecked = false;
}

void AggrItr::mark() {
    mainItr->mark();
}

void AggrItr::reset(const char i) {
    if (i == 0) {
        mainItr->reset(i);
        if (secondItr != NULL) {
            q->releaseItr(secondItr);
            secondItr = NULL;
        }
    } else {
        if (secondItr != NULL)
            secondItr->reset(i);
    }
    hasNextChecked = false;
}

void AggrItr::clear() {
    mainItr = NULL;
    secondItr = NULL;
}

void AggrItr::gotoFirstTerm(long c1) {
    mainItr->gotoFirstTerm(c1);
    hasNextChecked = true;
    n = mainItr->hasNext();
    if (n) {
        mainItr->next();
        if (mainItr->getValue1() < c1) { //End of the stream. Stop
            n = false;
        } else {
            if (!noSecColumn) {
                if (secondItr != NULL) {
                    q->releaseItr(secondItr);
                }
                setup_second_itr(idx);
                n = secondItr->hasNext();
                if (n)
                    secondItr->next();
            }
        }
    }
}

void AggrItr::gotoSecondTerm(long c2) {
    secondItr->gotoSecondTerm(c2);
    hasNextChecked = true;
    n = secondItr->hasNext();
    if (n)
        secondItr->next();
}

void AggrItr::init(int idx, PairItr* itr, Querier *q) {
    mainItr = itr;
    this->q = q;
    this->idx = idx;
    hasNextChecked = false;
    noSecColumn = false;
    secondItr = NULL;
    PairItr::setConstraint1(mainItr->getConstraint1());
    PairItr::setConstraint2(mainItr->getConstraint2());
}

uint64_t AggrItr::getCardinality() {
    if (n) {
        if (noSecColumn) {
            return mainItr->getCardinality();
        } else {
            uint64_t cardSec = secondItr->getCardinality();
            if (getConstraint1() >= 0) {
                return cardSec;
            } else {
                throw 10;
            }
        }
    } else {
        return 0;
    }
}

uint64_t AggrItr::estCardinality() {
    if (n) {
        if (noSecColumn) {
            return mainItr->estCardinality();
        } else {
            if (secondItr == NULL)
                setup_second_itr(idx);
            uint64_t cardSec = secondItr->estCardinality();
            if (getConstraint1() >= 0) {
                return cardSec;
            } else {
                uint64_t cardFirst = mainItr->estCardinality();
                return cardFirst * cardSec;
            }
        }
    } else {
        return 0;
    }
}
