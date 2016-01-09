/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Trident.

   Trident is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Trident is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Trident.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/tree/root.h>
#include <trident/storage/pairstorage.h>
#include <trident/storage/pairhandler.h>

#include <tridentcompr/utils/factory.h>

#include <boost/chrono.hpp>

#include <iostream>
#include <inttypes.h>
#include <cmath>

using namespace std;

class EMPTY_ITR: public PairItr {
public:
    int getType() {
        return 2;
    }

    long getValue1() {
        return 0;
    }

    long getValue2() {
        return 0;
    }

    bool has_next() {
        return false;
    }

    void next_pair() {
    }

    void clear() {
    }

    void mark() {
    }

    void reset(const char i) {
    }

    void move_first_term(long c1) {
    }

    void move_second_term(long c2) {
    }

    bool allowMerge() {
        return false;
    }

    uint64_t getCard() {
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
                 const long inputSize, const long nTerms, const int nindices, /*const bool aggregated,*/
                 CacheIdx *cachePSO, KB *sampleKB)
    : inputSize(inputSize), nTerms(nTerms), nindices(nindices), /*aggregated(aggregated),*/ cachePSO(cachePSO) {
    this->tree = tree;
    this->dict = dict;
    this->files = files;
    lastKeyFound = false;
    lastKeyQueried = -1;
    strat.init(&listFactory, &comprFactory, &list2Factory);

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
                                 const long third) {
//Check if first element is variable
    switch (idx) {
    case IDX_SPO:
    case IDX_SOP:
        if (first < 0)
            return inputSize;
        break;
    case IDX_POS:
    case IDX_PSO:
        if (second < 0)
            return inputSize;
        break;
    case IDX_OPS:
    case IDX_OSP:
        if (third < 0)
            return inputSize;
        break;
    }

    //The first term is a constant.
    PairItr *itr = get(idx, first, second, third);
    if (itr->getType() != 2 && itr->has_next()) {
        int countUnbound = 0;
        if (first < 0) countUnbound++;
        if (second < 0) countUnbound++;
        if (third < 0) countUnbound++;
        if (countUnbound == 2) {
            releaseItr(itr);
            return currentValue.getNElements(idx);
        } else if (countUnbound == 1) {
            uint64_t card = itr->getCard();
            releaseItr(itr);
            return card;
        } else {
            assert(countUnbound == 0);
            return 1;
        }
    } else {
        //EMPTY_ITR
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
    if (itr->getType() != 2 && itr->has_next()) {
        int countUnbound = 0;
        if (s < 0) countUnbound++;
        if (p < 0) countUnbound++;
        if (o < 0) countUnbound++;
        if (countUnbound == 0) {
            releaseItr(itr);
            return 1;
        } else if (countUnbound == 1) {
            uint64_t card = itr->getCard();
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

            PairHandler *ph = itr->getPairHandler();
            if (ph->getType() != COLUMN_LAYOUT) {
                throw 10; //not supported
            }

            long nElements = ((SimplifiedGroupPairHandler*)ph)->
                             getNFirstColumn();
            releaseItr(itr);
            return nElements;
        } else {
            throw 10; //Should never happen
        }


    } else {
        if (itr->getType() != 2)
            releaseItr(itr);
        return 0; //EMPTY_ITR. Join will fail...
    }
}

long Querier::getCard(const long s, const long p, const long o) {
    if (s < 0 && p < 0 && o < 0) {
        //They are all variables. Return the input size...
        return inputSize;
    }

    int idx = getIndex(s, p, o);
    PairItr *itr = get(idx, s, p, o);
    if (itr->getType() != 2 && itr->has_next()) {
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

        uint64_t card = itr->getCard();
        releaseItr(itr);
        return card;
    } else {
        if (itr->getType() != 2)
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
    if (itr->getType() != 2) {
        const bool resp = itr->has_next();
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

PairItr *Querier::get(const long s, const long p, const long o) {
    int idx = getIndex(s, p, o);
    return get(idx, s, p, o);
}


ArrayItr *Querier::getArrayIterator() {
    return factory2.get();
}

PairItr *Querier::getPairIterator(TermCoordinates * value, int perm, long v1,
                                  long v2) {
    if (value->getFileIdx(perm) == -1) {
        return NULL;
    } else {
        short fileIdx = value->getFileIdx(perm);
        int mark = value->getMark(perm);
        char strategy = value->getStrategy(perm);
        StorageItr *itr = factory1.get();
        itr->init(files[perm], fileIdx, mark, strat.getPairHandler(strategy),
                  v1, v2);
        return itr;
    }
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
    StorageItr *itr = factory1.get();
    itr->init(files[perm], fileIdx, pos, strat.getPairHandler(strategy),
              constraint1, -1);
    return itr;
}

PairItr *Querier::getPairIterator(const int perm, const long v1,
                                  const long v2) {
    if (currentValue.getFileIdx(perm) == -1) {
        return NULL;
    } else {
        short fileIdx = currentValue.getFileIdx(perm);
        int mark = currentValue.getMark(perm);
        char strategy = currentValue.getStrategy(perm);
        StorageItr *itr = factory1.get();
        itr->init(files[perm], fileIdx, mark, strat.getPairHandler(strategy),
                  v1, v2);
        return itr;
    }
}

PairItr *Querier::get(const int idx, const long s, const long p, const long o) {

    switch (idx) {
    case IDX_SPO:
        spo++;
        if (s >= 0) {
            if (lastKeyQueried != s) {
                lastKeyFound = tree->get(s, &currentValue);
                lastKeyQueried = s;
            }
            if (lastKeyFound && currentValue.exists(0)) {
                notAggrIndices++;
                PairItr *itr = getPairIterator(0, p, o);
                itr->setKey(s);
                return itr;
            } else {
                return &emptyItr;
            }
        } else {
            if (!copyRawData) {
                ScanItr *itr = factory3.get();
                itr->init(tree->itr(), this);
                return itr;
            } else {
                SimpleScanItr *itr = factory6.get();
                itr->init(this);
                return itr;
            }
        }
    case IDX_OPS:
        ops++;
        if (lastKeyQueried != o) {
            lastKeyFound = tree->get(o, &currentValue);
            lastKeyQueried = o;
        }
        if (lastKeyFound && currentValue.exists(1)) {
            notAggrIndices++;
            PairItr *itr = getPairIterator(1, p, s);
            itr->setKey(o);
            return itr;
        } else {
            return &emptyItr;
        }
    case IDX_POS:
        pos++;
        lastKeyFound = tree->get(p, &currentValue);
        lastKeyQueried = p;
        if (lastKeyFound && currentValue.exists(2)) {
            if (StorageStrat::isAggregated(currentValue.getStrategy(2))) {
                aggrIndices++;
                AggrItr *itr = factory4.get();
                itr->init(p, IDX_OPS, getPairIterator(2, o, s), this);
                itr->setKey(p);
                return itr;
            } else {
                notAggrIndices++;
                PairItr *itr = getPairIterator(2, o, s);
                itr->setKey(p);
                return itr;
            }
        } else {
            return &emptyItr;
        }
    case IDX_SOP:
        sop++;
        if (lastKeyQueried != s) {
            lastKeyFound = tree->get(s, &currentValue);
            lastKeyQueried = s;
        }
        if (lastKeyFound) {
            if (nindices == 3) {
                if (currentValue.exists(0)) {
                    PairItr *itr = getPairIterator(0, -1, -1);
                    itr->setKey(s);
                    //Prepare a fake iterator
                    PairItr *itr2 = newItrOnReverse(itr, o, p);
                    itr2->setKey(s);
                    releaseItr(itr);
                    return itr2;
                } else {
                    return &emptyItr;
                }
            } else { //nindices = 6
                if (currentValue.exists(3)) {
                    notAggrIndices++;
                    PairItr *itr = getPairIterator(3, o, p);
                    itr->setKey(s);
                    return itr;
                } else {
                    return &emptyItr;
                }
            }
        } else {
            return &emptyItr;
        }
    case IDX_OSP:
        osp++;
        if (lastKeyQueried != o) {
            lastKeyFound = tree->get(o, &currentValue);
            lastKeyQueried = o;
        }
        if (lastKeyFound) {
            if (nindices == 3) {
                BOOST_LOG_TRIVIAL(error) << "This shouldn't happen";
                throw 10;
                /*if (currentValue.exists(1)) {
                    CacheItr *itr = factory5.get();
                    itr->setKey(o);
                    return itr;
                } else {
                    return &emptyItr;
                }*/
            } else { //indices = 6
                if (currentValue.exists(4)) {
                    notAggrIndices++;
                    PairItr *itr = getPairIterator(4, s, p);
                    itr->setKey(o);
                    return itr;
                } else {
                    return &emptyItr;
                }
            }
        } else {
            return &emptyItr;
        }
    case IDX_PSO:
        pso++;
        lastKeyFound = tree->get(p, &currentValue);
        lastKeyQueried = p;
        if (lastKeyFound) {
            if (nindices == 3) {
                if (currentValue.exists(2)) {
                    if (s == -1 && o == -1) {
                        BOOST_LOG_TRIVIAL(error) << "The CacheIterator does work if c1 is not bound";
                        throw 10;
                    }
                    cacheIndices++;
                    CacheItr *itr = factory5.get();
                    itr->setKey(p);
                    itr->init(this, currentValue.getNElements(2), cachePSO, s, o);
                    return itr;
                } else {
                    return &emptyItr;
                }
            } else { // indices = 6
                if (currentValue.exists(5)) {
                    if (StorageStrat::isAggregated(currentValue.getStrategy(5))) {
                        aggrIndices++;
                        AggrItr *itr = factory4.get();
                        itr->init(p, IDX_SPO, getPairIterator(5, s, o), this);
                        itr->setKey(p);
                        return itr;
                    } else {
                        notAggrIndices++;
                        PairItr *itr = getPairIterator(5, s, o);
                        itr->setKey(p);
                        return itr;
                    }
                } else {
                    return &emptyItr;
                }
            }
        } else {
            return &emptyItr;
        }
    }
    return NULL;
}

//bool sortPairs(pair<uint64_t,uint64_t> i, pair<uint64_t,uint64_t> j) {
//  return ((i.second < j.second) && (i.first < j.first));
//}

PairItr *Querier::newItrOnReverse(PairItr * oldItr, const long v1, const long v2) {
    Pairs *tmpVector = new Pairs();
    while (oldItr->has_next()) {
        oldItr->next_pair();
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
    if (itr->getPairHandler() != NULL) {
        PairHandler *handler = itr->getPairHandler();
        handler->clear();
        switch (handler->getType()) {
        case ROW_LAYOUT:
            listFactory.release((ListPairHandler *) handler);
            break;
        case CLUSTER_LAYOUT:
            comprFactory.release((GroupPairHandler *) handler);
            break;
        case COLUMN_LAYOUT:
            list2Factory.release((SimplifiedGroupPairHandler*) handler);
            break;
        }
    }

    switch (itr->getType()) {
    case 0:
        factory1.release((StorageItr*) itr);
        break;
    case 1:
        itr->clear();
        factory2.release((ArrayItr*) itr);
        break;
    //case 2 is the empty iterator
    case 3:
        factory3.release((ScanItr*) itr);
        break;
    case 5:
        itr->clear();
        factory5.release((CacheItr*)itr);
        break;
    case 6:
        factory6.release((SimpleScanItr*)itr);
        break;
    case 4:
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
