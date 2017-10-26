#include <vlog/trident/tridenttupleitr.h>
#include <vlog/concepts.h>

#include <trident/sparql/sparqloperators.h>
#include <trident/kb/querier.h>
#include <trident/iterators/pairitr.h>

TridentTupleItr::TridentTupleItr() {}

void TridentTupleItr::init(Querier *querier, const VTuple *t,
        const std::vector<uint8_t> *fieldsToSort, bool onlyVars, std::mutex *mutex) {

    this->mutex = mutex;
    // this->onlyVars = onlyVars;
    this->querier = querier;
    long s, p, o;
    nvars = 0;
    if (t->get(0).isVariable()) {
        s = -1;
        nvars++;
    } else {
        s = (long)t->get(0).getValue();
    }
    if (t->get(1).isVariable()) {
        p = -1;
        nvars++;
    } else {
        p = (long)t->get(1).getValue();
    }
    if (t->get(2).isVariable()) {
        o = -1;
        nvars++;
    } else {
        o = (long)t->get(2).getValue();
    }



    if (mutex != NULL) {
        mutex->lock();
    }
    if (fieldsToSort == NULL) {
        idx = querier->getIndex(s, p, o);
    } else {
        //Assign variables names according to the sorting order
        int varid = -2;
        for (std::vector<uint8_t>::const_iterator itr = fieldsToSort->begin();
                itr != fieldsToSort->end(); ++itr) {
            switch (*itr) {
                case 0:
                    s = varid--;
                    break;
                case 1:
                    p = varid--;
                    break;
                case 2:
                    o = varid--;
                    break;
            }
        }
        idx = querier->getIndex(s, p, o);

        if (s < 0)
            s = -1;
        if (p < 0)
            p = -1;
        if (o < 0)
            o = -1;
    }
    // invPerm = querier->getInvOrder(idx);
    int *invPerm = querier->getInvOrder(idx);

    if (onlyVars) {
        int idx = 0;
        for (uint8_t j = 0; j < 3; ++j) {
            if (!t->get(j).isVariable()) {
                idx++;
            } else {
                varsPos[j - idx] = (uint8_t) invPerm[j];
            }
        }
        sizeTuple = (uint8_t) (3 - idx);
    } else {
        for (uint8_t j = 0; j < 3; ++j) {
            varsPos[j] = (uint8_t) invPerm[j];
        }
        sizeTuple = 3;
    }

    nextProcessed = false;
    nextOutcome = false;
    processedValues = 0;

    //If some variables have the same name, then we must change it
    equalFields = t->getRepeatedVars();
    physIterator = querier->get(idx, s, p, o);
    if (mutex != NULL) {
        mutex->unlock();
    }
}

bool TridentTupleItr::checkFields() {
    for (std::vector<std::pair<uint8_t, uint8_t>>::const_iterator itr =
            equalFields.begin();
            itr != equalFields.end(); ++itr) {
        if (getElementAt(itr->first) != getElementAt(itr->second)) {
            return false;
        }
    }
    return true;
}

bool TridentTupleItr::hasNext() {
    if (!nextProcessed) {
        if (physIterator->hasNext()) {
            physIterator->next();
            nextOutcome = true;
            if (equalFields.size() > 0) {
                while (!checkFields()) {
                    if (!physIterator->hasNext()) {
                        nextOutcome = false;
                        break;
                    } else {
                        physIterator->next();
                    }
                }
            }
        } else {
            nextOutcome = false;
        }
        nextProcessed = true;
    }
    return nextOutcome;
}

void TridentTupleItr::next() {
    if (nextProcessed) {
        nextProcessed = false;
    } else {
        physIterator->next();
    }
}

size_t TridentTupleItr::getTupleSize() {
    return sizeTuple;
}

uint64_t TridentTupleItr::getElementAt(const int p) {
    // const uint8_t pos = onlyVars ? varsPos[p] : (uint8_t) invPerm[p];
    const uint8_t pos = varsPos[p];

    switch (pos) {
        case 0:
            return physIterator->getKey();
        case 1:
            return physIterator->getValue1();
        case 2:
            return physIterator->getValue2();
    }
    LOG(ERRORL) << "This should not happen";
    throw 10;
}

TridentTupleItr::~TridentTupleItr() {
    if (querier != NULL) {
        if (mutex != NULL) {
            std::lock_guard<std::mutex> lock(*mutex);
            querier->releaseItr(physIterator);
        } else {
            querier->releaseItr(physIterator);
        }
    }
}

void TridentTupleItr::clear() {
    if (mutex != NULL) {
        std::lock_guard<std::mutex> lock(*mutex);
        if (querier != NULL) {
            querier->releaseItr(physIterator);
        }
    } else {
        if (querier != NULL) {
            querier->releaseItr(physIterator);
        }
    }
    physIterator = NULL;
    querier = NULL;
}

const char* TridentTupleItr::getUnderlyingArray(uint8_t column) {
    // const uint8_t pos = onlyVars ? varsPos[column] : (uint8_t) invPerm[column];
    // return NULL;
    const uint8_t pos = varsPos[column];
    if (pos == 0 || nvars == 3) {
        // Can happen if asking for TE(?,?,?)
        return NULL;
    }
    switch (pos) {
        case 1:
            return ((NewColumnTable*)physIterator)->getUnderlyingArray(1);
        case 2:
            return ((NewColumnTable*)physIterator)->getUnderlyingArray(2);
    }
    LOG(ERRORL) << "This should not happen";
    throw 10;
}

size_t TridentTupleItr::getCardinality() {
    if (mutex != NULL) {
        std::lock_guard<std::mutex> lock(*mutex);
        NewColumnTable *nct = (NewColumnTable*) physIterator;
        return nct->getCardinality();
    }
    NewColumnTable *nct = (NewColumnTable*) physIterator;
    return nct->getCardinality();
}

std::pair<uint8_t, std::pair<uint8_t, uint8_t>> TridentTupleItr::getSizeElemUnderlyingArray(uint8_t column) {
    // const uint8_t pos = onlyVars ? varsPos[column] : (uint8_t) invPerm[column];
    const uint8_t pos = varsPos[column];
    NewColumnTable *nct = (NewColumnTable*) physIterator;
    switch (pos) {
        case 0:
            throw 10;
        case 1:
            return make_pair(nct->getReaderSize1(), std::make_pair(nct->getReaderCountSize(), nct->getReaderStartingPointSize()));
        case 2:
            return make_pair(nct->getReaderSize2(), std::make_pair(0, 0));
    }
    LOG(ERRORL) << "This should not happen";
    throw 10;
}
