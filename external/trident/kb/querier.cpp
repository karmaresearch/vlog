#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/tree/root.h>
#include <trident/binarytables/tableshandler.h>
#include <trident/binarytables/binarytable.h>

#include <tridentcompr/utils/factory.h>

#include <boost/chrono.hpp>

#include <iostream>
#include <inttypes.h>
#include <cmath>

using namespace std;

class EmptyItr: public PairItr {
public:
    int getTypeItr() {
        return EMPTY_ITR;
    }

    long getValue1() {
        return 0;
    }

    long getValue2() {
        return 0;
    }

    long getCount() {
        return 0;
    }

    bool hasNext() {
        return false;
    }

    void next() {
    }

    void ignoreSecondColumn() {
    }

    void clear() {
    }

    void mark() {
    }

    void reset(const char i) {
    }

    void gotoFirstTerm(long c1) {
    }

    void gotoSecondTerm(long c2) {
    }

    bool allowMerge() {
        return false;
    }

    uint64_t estCardinality() {
        return 0;
    }

    uint64_t getCardinality() {
        return 0;
    }
} emptyItr;

int PERM_SPO[] = { 0, 1, 2 };
int INV_PERM_SPO[] = { 0, 1, 2 };
int PERM_OPS[] = { 2, 1, 0 };
int INV_PERM_OPS[] = { 2, 1, 0 };
int PERM_POS[] = { 1, 2, 0 };
int INV_PERM_POS[] = { 2, 0, 1 };

int PERM_PSO[] = { 1, 0, 2 };
int INV_PERM_PSO[] = { 1, 0, 2 };
int PERM_OSP[] = { 2, 0, 1 };
int INV_PERM_OSP[] = { 1, 2, 0 };
int PERM_SOP[] = { 0, 2, 1 };
int INV_PERM_SOP[] = { 0, 2, 1 };

Querier::Querier(Root* tree, DictMgmt *dict, TableStorage** files,
                 const long inputSize, const long nTerms, const int nindices,
                 const long *nTablesPerPartition,
                 const long *nFirstTablesPerPartition, KB *sampleKB)
    : inputSize(inputSize), nTerms(nTerms),
      nTablesPerPartition(nTablesPerPartition),
      nFirstTablesPerPartition(nFirstTablesPerPartition), nindices(nindices) {
    this->tree = tree;
    this->dict = dict;
    this->files = files;
    lastKeyFound = false;
    lastKeyQueried = -1;
    strat.init(&listFactory, &comprFactory, &list2Factory, &ncFactory,
               NULL, NULL, NULL, NULL);

    std::string pathFirstPerm = files[0]->getPath();
    pathRawData = pathFirstPerm + std::string("raw");
    copyRawData = fs::exists(pathRawData);

    if (sampleKB != NULL) {
        sampler = std::unique_ptr<Querier>(sampleKB->query());
    }
}

char Querier::getStrategy(const int idx, const long v) {
    if (lastKeyQueried != v) {
        lastKeyFound = tree->get(v, &currentValue);
        lastKeyQueried = v;
    }
    return currentValue.getStrategy(idx);
}

uint64_t Querier::getCard(const int idx, const long v) {
    if (lastKeyQueried != v) {
        lastKeyFound = tree->get(v, &currentValue);
        lastKeyQueried = v;
    }
    return currentValue.getNElements(idx);
}

uint64_t Querier::getCardOnIndex(const int idx, const long first, const long second,
                                 const long third, bool skipLast) {
    //Check if first element is variable
    switch (idx) {
    case IDX_SPO:
    case IDX_SOP:
        if (first < 0 && !skipLast)
            return inputSize;
        break;
    case IDX_POS:
    case IDX_PSO:
        if (second < 0 && !skipLast)
            return inputSize;
        break;
    case IDX_OPS:
    case IDX_OSP:
        if (third < 0 && !skipLast)
            return inputSize;
        break;
    }

    //The first term is a constant.
    PairItr *itr = get(idx, first, second, third);
    if (itr->getTypeItr() != EMPTY_ITR && itr->hasNext()) {
        int countUnbound = 0;
        if (first < 0) countUnbound++;
        if (second < 0) countUnbound++;
        if (third < 0) countUnbound++;
        if (countUnbound >= 2) {
            if (skipLast) {
                itr->ignoreSecondColumn();
                uint64_t card = itr->getCardinality();
                releaseItr(itr);
                return card;
            } else {
                releaseItr(itr);
                if (currentValue.exists(idx)) {
                    return currentValue.getNElements(idx);
                } else if (idx > 2) {
                    return currentValue.getNElements(idx - 3);
                } else {
                    throw 10; //should not happen
                }
            }
        } else if (countUnbound == 1) {
            uint64_t card;
            if (skipLast) {
                card = 1;
            } else {
                card = itr->getCardinality();
            }
            releaseItr(itr);
            return card;
        } else {
            assert(countUnbound == 0);
            releaseItr(itr);
            return 1;
        }
    } else {
        //No results
        if (itr->getTypeItr() != EMPTY_ITR) {
            releaseItr(itr);
        }
        return 0;
    }
}

long Querier::getCard(const long s, const long p, const long o, uint8_t pos) {
    if (s < 0 && p < 0 && o < 0) {
        throw 10; //not supported
    }

    //At least one variable is bound. Thus "pos" must refer to another one
    int idx = getIndex(s, p, o);
    PairItr *itr = get(idx, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR && itr->hasNext()) {
        int countUnbound = 0;
        if (s < 0) countUnbound++;
        if (p < 0) countUnbound++;
        if (o < 0) countUnbound++;
        if (countUnbound == 0) {
            releaseItr(itr);
            return 1;
        } else if (countUnbound == 1) {
            uint64_t card = itr->getCardinality();
            releaseItr(itr);
            return card;
        } else if (countUnbound == 2) {
            //I must count the number of first columns
            int idx2 = IDX_SPO;
            if (s >= 0) {
                if (pos == 2) {
                    idx2 = IDX_SOP;
                }
            } else if (p >= 0) {
                if (pos == 0) {
                    idx2 = IDX_PSO;
                } else {
                    idx2 = IDX_POS;
                }
            } else { //o>= 0
                assert(o >= 0);
                if (pos == 0) {
                    idx2 = IDX_OSP;
                } else {
                    idx2 = IDX_OPS;
                }
            }
            if (idx != idx2) {
                releaseItr(itr);
                itr = get(idx2, s, p, o);
            }

            //BinaryTable *ph = itr->getBinaryTable();
            itr->ignoreSecondColumn();
            long nElements = itr->getCardinality();
            releaseItr(itr);
            return nElements;
        } else {
            throw 10; //Should never happen
        }


    } else {
        if (itr->getTypeItr() != EMPTY_ITR)
            releaseItr(itr);
        return 0; //EMPTY_ITR. Join will fail...
    }
}

uint64_t Querier::isAggregated(const int idx, const long first, const long second,
                               const long third) {
    if (idx != IDX_POS && idx != IDX_PSO)
        return 0;

    const long key = second;
    if (key >= 0) {
        if (lastKeyQueried != key) {
            lastKeyFound = tree->get(key, &currentValue);
            lastKeyQueried = key;
        }
        if (currentValue.exists(idx)) {
            if (StorageStrat::isAggregated(currentValue.getStrategy(idx))) {
                //Is the second term bound?
                if (idx == IDX_PSO && first >= 0) {
                    return 1;
                } else if (idx == IDX_POS && third >= 0) {
                    return 1;
                } else {
                    PairItr *itr = getPairIterator(&currentValue, idx, key, -1, -1,
                                                   true, true);
                    itr->ignoreSecondColumn();
                    uint64_t count = itr->getCount();
                    releaseItr(itr);
                    return count;
                }
            }
        }
    } else {
        //Is it all aggregated?
    }
    return 0;
}

uint64_t Querier::isReverse(const int idx, const long first, const long second,
                            const long third) {
    if (idx < 3)
        return 0;

    long key1;
    switch (idx) {
    case IDX_SPO:
    case IDX_SOP:
        if (first < 0)
            return inputSize;
        key1 = first;
        break;
    case IDX_POS:
    case IDX_PSO:
        if (second < 0)
            return inputSize;
        key1 = second;
        break;
    case IDX_OPS:
    case IDX_OSP:
        if (third < 0)
            return inputSize;
        key1 = third;
        break;
    }

    //Check key
    if (lastKeyQueried != key1) {
        lastKeyFound = tree->get(key1, &currentValue);
        lastKeyQueried = key1;
    }
    if (!currentValue.exists(idx)) {
        if (currentValue.exists(idx - 3)) {
            return currentValue.getNElements(idx - 3);
        }
    }
    return 0;
}

uint64_t Querier::estCardOnIndex(const int idx, const long first, const long second,
                                 const long third) {
    //Check if first element is variable
    long key1, key2;
    switch (idx) {
    case IDX_SPO:
    case IDX_SOP:
        if (first < 0)
            return inputSize;
        key1 = first;
        if (idx == IDX_SPO) {
            key2 = second;
        } else {
            key2 = third;
        }
        break;
    case IDX_POS:
    case IDX_PSO:
        if (second < 0)
            return inputSize;
        key1 = second;
        if (idx == IDX_POS)
            key2 = third;
        else
            key2 = first;
        break;
    case IDX_OPS:
    case IDX_OSP:
        if (third < 0)
            return inputSize;
        key1 = third;
        if (idx == IDX_OPS)
            key2 = second;
        else
            key2 = first;
        break;
    }

    //The first term is a constant.
    int countUnbound = 0;
    if (first < 0) countUnbound++;
    if (second < 0) countUnbound++;
    if (third < 0) countUnbound++;
    if (countUnbound == 0) {
        return 1;
    } else if (countUnbound == 1 && key2 >= 0) {
        PairItr *itr = get(idx, first, second, third);
        long card = itr->estCardinality();
        releaseItr(itr);
        return card;
    } else {
        if (lastKeyQueried != key1) {
            lastKeyFound = tree->get(key1, &currentValue);
            lastKeyQueried = key1;
        }
        int perm = idx;
        if (perm > 2)
            perm = perm - 3;
        if (currentValue.exists(perm)) {
            return currentValue.getNElements(perm);
        } else {
            return 0;
        }
    }
}

long Querier::estCard(const long s, const long p, const long o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        return inputSize;
    }

    int countUnbound = 0;
    if (s < 0) countUnbound++;
    if (p < 0) countUnbound++;
    if (o < 0) countUnbound++;
    if (countUnbound == 0) {
        return 1;
    } else if (countUnbound == 1) {
        int perm = getIndex(s, p, o);
        PairItr *itr = get(perm, s, p, o);
        long card = itr->estCardinality();
        releaseItr(itr);
        return card;
    } else if (countUnbound == 2) {
        long key;
        int perm;
        if (s >= 0) {
            key = s;
            perm = IDX_SPO;
        } else if (p >= 0) {
            key = p;
            perm = IDX_POS;
        } else {
            key = o;
            perm = IDX_OPS;
        }
        if (lastKeyQueried != key) {
            lastKeyFound = tree->get(key, &currentValue);
            lastKeyQueried = key;
        }
        if (currentValue.exists(perm)) {
            return currentValue.getNElements(perm);
        } else {
            return 0;
        }
    }
    throw 10;
}

long Querier::getCard(const long s, const long p, const long o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        return inputSize;
    }

    int idx = getIndex(s, p, o);
    PairItr *itr = get(idx, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR && itr->hasNext()) {
        int countUnbound = 0;
        if (s < 0) countUnbound++;
        if (p < 0) countUnbound++;
        if (o < 0) countUnbound++;
        if (countUnbound == 0) {
            releaseItr(itr);
            return 1;
        }

        if (countUnbound == 2) {
            releaseItr(itr);
            return currentValue.getNElements(idx);
        }

        itr->next();
        uint64_t card = itr->getCardinality();
        releaseItr(itr);
        return card;
    } else {
        if (itr->getTypeItr() != EMPTY_ITR)
            releaseItr(itr);
        return 0; //EMPTY_ITR. Join will fail...
    }
}

bool Querier::isEmpty(const long s, const long p, const long o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        return inputSize == 0;
    }

    int idx = getIndex(s, p, o);
    PairItr *itr = get(idx, s, p, o);
    if (itr->getTypeItr() != EMPTY_ITR) {
        const bool resp = itr->hasNext();
        releaseItr(itr);
        return !resp;
    } else {
        return true;
    }
}

int Querier::getIndex(long s, long p, long o) {
    if (nindices == 1) {
        return IDX_SPO;
    }

    if (s >= 0) {
        //SPO or SOP
        if (p >= 0 || p == -2 || o == -1) {
            return IDX_SPO;
        } else {
            return IDX_SOP;
        }
    }

    if (o >= 0) {
        //OPS or OSP
        if (p >= 0 || p == -2 || s == -1) {
            return IDX_OPS;
        } else {
            if (nindices == 3) {
                return IDX_SOP;
            } else {
                return IDX_OSP;
            }
        }
    }

    if (p >= 0) {
        //POS or PSO
        if (o >= 0 || o == -2 || s == -1) {
            return IDX_POS;
        } else {
            return IDX_PSO;
        }
    }

    //No constant on the pattern. Either they are all -1, or there is a join
    if (s == -2) {
        if (p != -1 || o == -1) {
            return IDX_SPO;
        } else {
            return IDX_SOP;
        }
    }

    if (o == -2) {
        if (p != -1 || s == -1) {
            return IDX_OPS;
        } else {
            if (nindices == 3) {
                return IDX_SOP;
            } else {
                return IDX_OSP;
            }
        }
    }

    if (p == -2) {
        if (o != -1 || s == -1) {
            return IDX_POS;
        } else {
            return IDX_PSO;
        }
    }

    //Scan
    return IDX_SPO;
}

ArrayItr *Querier::getArrayIterator() {
    return factory2.get();
}

PairItr *Querier::getPairIterator(TermCoordinates * value, int perm,
                                  const long key,
                                  long v1,
                                  long v2,
                                  const bool constrain,
                                  const bool noAggr) {
    short fileIdx = value->getFileIdx(perm);
    int mark = value->getMark(perm);
    char strategy = value->getStrategy(perm);
    return get(perm, key, fileIdx, mark, strategy, v1, v2, constrain, noAggr);
}

void Querier::initBinaryTable(TableStorage *storage,
                              int file,
                              int mark,
                              BinaryTable *t,
                              long v1,
                              long v2,
                              const bool setConstraints) {
    storage->setupPairHandler(t, file, mark);
    if (v1 != -1) {
        t->moveToClosestFirstTerm(v1);
        if (t->getValue1() == v1 && v2 != -1) {
            t->moveToClosestSecondTerm(v1, v2);
        }
    }
    //Check whether the current line in the table respects the constraints.
    if (setConstraints) {
        t->setConstraint1(v1);
        t->setConstraint2(v2);
    } else {
        t->setConstraint1(-1);
        t->setConstraint2(-1);
    }
    t->setNextFromConstraints();
}

Querier &Querier::getSampler() {
    if (sampler == NULL) {
        BOOST_LOG_TRIVIAL(error) << "No sampler available";
        throw 10;
    } else {
        return *sampler;
    }
}

PairItr *Querier::getFilePairIterator(const int perm, const long constraint1,
                                      const char strategy, const short fileIdx, const int pos) {
    BinaryTable *t = strat.getBinaryTable(strategy);
    initBinaryTable(files[perm], fileIdx, pos, t, constraint1, -1, true);
    return t;
}

PairItr *Querier::getPermuted(const int idx, const long el1, const long el2,
                              const long el3, const bool constrain) {
    switch (idx) {
    case IDX_SPO:
        return get(idx, el1, el2, el3, constrain);
    case IDX_SOP:
        return get(idx, el1, el3, el2, constrain);
    case IDX_POS:
        return get(idx, el3, el1, el2, constrain);
    case IDX_PSO:
        return get(idx, el2, el1, el3, constrain);
    case IDX_OSP:
        return get(idx, el2, el3, el1, constrain);
    case IDX_OPS:
        return get(idx, el3, el2, el1, constrain);
    }
    throw 10;
}

bool Querier::permExists(const int perm) const {
    return files[perm] != NULL;
}

TermItr *Querier::getTermList(const int perm, const bool enforcePerm) {
    TableStorage *storage;
    if (perm > 2 && !enforcePerm) {
        storage = files[perm - 3];
    } else {
        storage = files[perm];
    }

    if (storage != NULL) {
        TermItr *itr = factory7.get();
        itr->init(storage, nTablesPerPartition[perm]);
        return itr;
    } else {
        return NULL;
    }
}

PairItr *Querier::get(const int idx, TermCoordinates &value,
                      const long key, const long v1,
                      const long v2, const bool cons) {

    if (value.exists(idx)) {
        if (StorageStrat::isAggregated(value.getStrategy(idx))) {
            aggrIndices++;
            AggrItr *itr = factory4.get();
            itr->init(idx, getPairIterator(&value, idx, key, v1, v2,
                                           true, true),
                      this);
            itr->setKey(key);
            return itr;
        } else {
            notAggrIndices++;
            PairItr *itr = getPairIterator(&value, idx, key, v1, v2, cons,
                                           false);
            //itr->setKey(key);
            return itr;
        }
    } else if (idx - 3 >= 0 && value.exists(idx - 3)) {
        PairItr *itr = get(idx - 3, value, key, -1, -1, cons);
        PairItr *itr2 = newItrOnReverse(itr, v1, v2);
        itr2->setKey(key);
        releaseItr(itr);
        return itr2;
    } else {
        return &emptyItr;
    }
}

PairItr *Querier::get(const int perm,
                      const long key,
                      const short fileIdx,
                      const int mark,
                      const char strategy,
                      const long v1,
                      const long v2,
                      const bool constrain,
                      const bool noAggr) {
    BinaryTable *itr = strat.getBinaryTable(strategy);
    initBinaryTable(files[perm], fileIdx, mark, itr, v1, v2, constrain);
    if (StorageStrat::isAggregated(strategy) && !noAggr) {
        AggrItr *itr2 = factory4.get();
        itr2->init(perm, itr, this);
        itr2->setKey(key);
        return itr2;
    } else {
        itr->setKey(key);
        return itr;
    }
}


PairItr *Querier::get(const int idx, const long s, const long p, const long o,
                      const bool cons) {
    switch (idx) {
    case IDX_SPO:
        spo++;
        if (s >= 0) {
            if (lastKeyQueried != s) {
                lastKeyFound = tree->get(s, &currentValue);
                lastKeyQueried = s;
            }
            if (lastKeyFound) {
                return get(IDX_SPO, currentValue, s, p, o, cons);
            } else {
                return &emptyItr;
            }
        } else {
            //if (!copyRawData) {
            ScanItr *itr = factory3.get();
            itr->init(IDX_SPO, this);
            return itr;
            //} else {
            //    SimpleScanItr *itr = factory6.get();
            //    itr->init(this);
            //    return itr;
            //}
        }
    case IDX_OPS:
        ops++;
        if (o >= 0) {
            if (lastKeyQueried != o) {
                lastKeyFound = tree->get(o, &currentValue);
                lastKeyQueried = o;
            }
            if (lastKeyFound) {
                return get(IDX_OPS, currentValue, o, p, s, cons);
            } else {
                return &emptyItr;
            }
        } else {
            ScanItr *itr = factory3.get();
            itr->init(IDX_OPS, this);
            return itr;
        }
    case IDX_POS:
        pos++;
        if (p >= 0) {
            lastKeyFound = tree->get(p, &currentValue);
            lastKeyQueried = p;
            if (lastKeyFound) {
                return get(IDX_POS, currentValue, p, o, s, cons);
            } else {
                return &emptyItr;
            }
        } else {
            ScanItr *itr = factory3.get();
            itr->init(IDX_POS, this);
            return itr;
        }
    case IDX_SOP:
        sop++;
        if (s >= 0) {
            if (lastKeyQueried != s) {
                lastKeyFound = tree->get(s, &currentValue);
                lastKeyQueried = s;
            }
            if (lastKeyFound) {
                return get(IDX_SOP, currentValue, s, o, p, cons);
            } else {
                return &emptyItr;
            }
        } else {
            ScanItr *itr = factory3.get();
            itr->init(IDX_SOP, this);
            return itr;
        }
    case IDX_OSP:
        osp++;
        if (o >= 0) {
            if (lastKeyQueried != o) {
                lastKeyFound = tree->get(o, &currentValue);
                lastKeyQueried = o;
            }
            if (lastKeyFound) {
                return get(IDX_OSP, currentValue, o, s, p, cons);
            } else {
                return &emptyItr;
            }
        } else {
            ScanItr *itr = factory3.get();
            itr->init(IDX_OSP, this);
            return itr;
        }
    case IDX_PSO:
        pso++;
        if (p >= 0) {
            lastKeyFound = tree->get(p, &currentValue);
            lastKeyQueried = p;
            if (lastKeyFound) {
                return get(IDX_PSO, currentValue, p, s, o, cons);
            } else {
                return &emptyItr;
            }
        } else {
            ScanItr *itr = factory3.get();
            itr->init(IDX_PSO, this);
            return itr;
        }
    }
    return NULL;
}

PairItr *Querier::newItrOnReverse(PairItr * oldItr, const long v1, const long v2) {
    Pairs *tmpVector = new Pairs();
    while (oldItr->hasNext()) {
        oldItr->next();
        if (v1 < 0 || oldItr->getValue2() == v1) {
            tmpVector->push_back(
                std::pair<uint64_t, uint64_t>(oldItr->getValue2(),
                                              oldItr->getValue1()));
        }
    }

    if (tmpVector->size() > 0) {
        std::sort(tmpVector->begin(), tmpVector->end());
        ArrayItr *itr = factory2.get();
        itr->init(tmpVector, v1, v2);
        return itr;
    } else {
        return &emptyItr;
    }
}

int *Querier::getOrder(int idx) {
    switch (idx) {
    case IDX_SPO:
        return PERM_SPO;
    case IDX_OPS:
        return PERM_OPS;
    case IDX_POS:
        return PERM_POS;
    case IDX_OSP:
        return PERM_OSP;
    case IDX_PSO:
        return PERM_PSO;
    case IDX_SOP:
        return PERM_SOP;
    }
    return NULL;
}

int *Querier::getInvOrder(int idx) {
    switch (idx) {
    case IDX_SPO:
        return INV_PERM_SPO;
    case IDX_OPS:
        return INV_PERM_OPS;
    case IDX_POS:
        return INV_PERM_POS;
    case IDX_OSP:
        return INV_PERM_OSP;
    case IDX_PSO:
        return INV_PERM_PSO;
    case IDX_SOP:
        return INV_PERM_SOP;
    }
    return NULL;
}

void Querier::releaseItr(PairItr * itr) {
    switch (itr->getTypeItr()) {
    case ROW_ITR:
        ((BinaryTable*)itr)->clear();
        listFactory.release((RowTable *) itr);
        break;
    case CLUSTER_ITR:
        ((BinaryTable*)itr)->clear();
        comprFactory.release((ClusterTable *) itr);
        break;
    case COLUMN_ITR:
        ((BinaryTable*)itr)->clear();
        list2Factory.release((ColumnTable *) itr);
        break;
    case NEWCOLUMN_ITR:
        ((BinaryTable*)itr)->clear();
        ncFactory.release((NewColumnTable *) itr);
        break;
    case ARRAY_ITR:
        itr->clear();
        factory2.release((ArrayItr*) itr);
        break;
    case SCAN_ITR:
        factory3.release((ScanItr*) itr);
        break;
    case CACHE_ITR:
        itr->clear();
        factory5.release((CacheItr*)itr);
        break;
    case SIMPLESCAN_ITR:
        factory6.release((SimpleScanItr*)itr);
        break;
    case TERM_ITR:
        factory7.release((TermItr*)itr);
        break;
    case AGGR_ITR:
        AggrItr *citr = (AggrItr*) itr;
        if (citr->getMainItr() != NULL)
            releaseItr(citr->getMainItr());
        if (citr->getSecondItr() != NULL)
            releaseItr(citr->getSecondItr());
        citr->clear();
        factory4.release(citr);
        break;
    }
}
