#include <trident/iterators/scanitr.h>
#include <trident/tree/treeitr.h>
#include <trident/kb/querier.h>

#include <trident/binarytables/storagestrat.h>

#include <iostream>

using namespace std;

void ScanItr::init(int idx, Querier *q) {
    this->idx = idx;
    this->q = q;
    currentTable = NULL;
    reversedItr = NULL;
    //hasNextChecked = false;

    itr1 = q->getTermList(idx, true);
    if (idx > 2 || itr1 == NULL) {
        itr2 = q->getTermList(idx - 3);
        if (itr1) {
            if (itr1->hasNext()) {
                itr1->next();
            } else {
                q->releaseItr(itr1);
                itr1 = NULL;
            }
        }
    } else
        itr2 = NULL;
    storage = q->getTableStorage(idx);
    strat = q->getStorageStrat();
    ignseccolumn = false;
}

uint64_t ScanItr::getCardinality() {
    if (ignseccolumn) {
        return q->getNFirstTablesPerPartition(idx);
    } else {
        return q->getInputSize();
    }
}

uint64_t ScanItr::estCardinality() {
    if (ignseccolumn) {
        return q->getNFirstTablesPerPartition(idx);
    } else {
        return q->getInputSize();
    }
}

bool ScanItr::hasNext() {
    if (currentTable != NULL) {
        if (currentTable->hasNext()) {
            return true;
        } else {
            q->releaseItr(currentTable);
            currentTable = NULL;
        }
    } else if (reversedItr != NULL) {
        if (reversedItr->hasNext()) {
            return true;
        } else {
            q->releaseItr(reversedItr);
            reversedItr = NULL;
        }
    }

    //Move to the next key
    if (itr2) {
        if (itr2->hasNext()) {
            itr2->next();
            const long key2 = itr2->getKey();
            //itr1 can be either equal, greater or being finished
            if (itr1) {
                if (itr1->getKey() < key2) {
                    if (itr1->hasNext()) {
                        itr1->next();
                    } else {
                        q->releaseItr(itr1);
                        itr1 = NULL;
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    } else {
        if (itr1->hasNext()) {
            itr1->next();
            return true;
        } else {
            return false;
        }
    }
}

void ScanItr::next() {
    if (currentTable == NULL) {
        if (reversedItr == NULL) {
            if (itr2 && (!itr1 || itr2->getKey() != itr1->getKey())) {
                const long key = itr2->getKey();
                char strategy = itr2->getCurrentStrat();
                short file = itr2->getCurrentFile();
                int mark = itr2->getCurrentMark();
                //cerr << "New reversed iterator for key " << key << " itr1=" << itr1->getKey() << " mark=" << mark << " file=" << file << endl;
                //Need to get reversed table
                PairItr *itr = q->get(idx - 3, key, file, mark, strategy,
                                      -1, -1, false, false);
                reversedItr = q->newItrOnReverse(itr, -1, -1);
                if (ignseccolumn)
                    reversedItr->ignoreSecondColumn();
                if (!reversedItr->hasNext()) {
                    throw 10; //should not happen
                }
                reversedItr->next();
                q->releaseItr(itr);
                setKey(key);
            } else {
                const long key = itr1->getKey();
                char strategy = itr1->getCurrentStrat();
                short file = itr1->getCurrentFile();
                int mark = itr1->getCurrentMark();
                //cerr << "New table for key " << key << " itr2 " << itr2->getKey() << " mark=" << mark << " file=" << file << endl;
                currentTable = q->get(idx, key, file, mark, strategy,
                                      -1, -1, false, false);
                if (ignseccolumn)
                    currentTable->ignoreSecondColumn();
                setKey(key);
                if (!currentTable->hasNext()) {
                    throw 10;
                }
                currentTable->next();
            }
        } else {
            reversedItr->next();
        }
    } else {
        currentTable->next();
    }
}

void ScanItr::clear() {
    if (currentTable)
        q->releaseItr(currentTable);
    if (itr1)
        q->releaseItr(itr1);
    if (itr2)
        q->releaseItr(itr2);
    if (reversedItr)
        q->releaseItr(reversedItr);
}

void ScanItr::mark() {
}

void ScanItr::reset(const char i) {
}

void ScanItr::gotoKey(long k) {
    if (k > getKey()) {
        if (currentTable) {
            //release it
            q->releaseItr(currentTable);
            currentTable = NULL;
        }
        if (reversedItr) {
            q->releaseItr(reversedItr);
            reversedItr = NULL;
        }

        //Move forward
        if (itr2) {
            itr2->gotoKey(k);
            //I do not advance this iteration because it will be increased
            //the first time we call the hasNext() method.
        }
        itr1->gotoKey(k);
    }
}

void ScanItr::gotoFirstTerm(long c1) {
    assert(reversedItr != NULL || currentTable != NULL);
    if (reversedItr) {
        reversedItr->gotoFirstTerm(c1);
    } else {
        currentTable->gotoFirstTerm(c1);
    }

}

void ScanItr::gotoSecondTerm(long c2) {
    assert(reversedItr != NULL || currentTable != NULL);
    if (reversedItr) {
        reversedItr->gotoSecondTerm(c2);
    } else {
        currentTable->gotoSecondTerm(c2);
    }
}
