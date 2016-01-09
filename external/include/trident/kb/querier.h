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

#ifndef QUERIER_H_
#define QUERIER_H_

#include <trident/iterators/storageitr.h>
#include <trident/iterators/arrayitr.h>
#include <trident/iterators/scanitr.h>
#include <trident/iterators/simplescanitr.h>
#include <trident/iterators/aggritr.h>
#include <trident/iterators/cacheitr.h>
#include <trident/tree/coordinates.h>
#include <trident/storage/storagestrat.h>

#include <tridentcompr/utils/factory.h>

#include <boost/chrono.hpp>

#include <iostream>

namespace timens = boost::chrono;

class TableStorage;
class ListPairHandler;
class GroupPairHandler;
class Root;
class DictMgmt;
class CacheIdx;
class KB;

class Querier {
private:
    Root* tree;
    DictMgmt *dict;
    TableStorage **files;
    long lastKeyQueried;
    bool lastKeyFound;
    const long inputSize;
    const long nTerms;

    std::string pathRawData;
    bool copyRawData;

    const int nindices;
    CacheIdx *cachePSO;

    std::unique_ptr<Querier> sampler;

    TermCoordinates currentValue;

    //Factories
    Factory<StorageItr> factory1;
    Factory<ArrayItr> factory2;
    Factory<ScanItr> factory3;
    Factory<AggrItr> factory4;
    Factory<CacheItr> factory5;
    Factory<SimpleScanItr> factory6;
    Factory<ListPairHandler> listFactory;
    Factory<GroupPairHandler> comprFactory;
    Factory<SimplifiedGroupPairHandler> list2Factory;
    StorageStrat strat;

    //Statistics
    long aggrIndices, notAggrIndices, cacheIndices;
    long spo, ops, pos, sop, osp, pso;

    PairItr *getPairIterator(const int perm, const long c1, const long c2);

    PairItr *newItrOnReverse(PairItr *itr, const long v1, const long v2);
public:

    struct Counters {
        long statsRow;
        long statsColumn;
        long statsCluster;
        long aggrIndices;
        long notAggrIndices;
        long cacheIndices;
        long spo, ops, pos, sop, osp, pso;
    };

    Querier(Root* tree, DictMgmt *dict, TableStorage** files, const long inputSize,
            const long nTerms, const int nindices, /*const bool aggregated,*/ CacheIdx *cachePSO,
            KB *sampleKB);

    PairItr *get(const long s, const long p, const long o);

    PairItr *get(const int idx, const long s, const long p, const long o);

    uint64_t getCardOnIndex(const int idx, const long first, const long second, const long third);

    long getCard(const long s, const long p, const long o);

    long getCard(const long s, const long p, const long o, uint8_t pos);

    bool isEmpty(const long s, const long p, const long o);

    int getIndex(const long s, const long p, const long o);

    char getStrategy(const int idx, const long v);

    uint64_t getCard(const int idx, const long v);

    Querier &getSampler();

    int *getInvOrder(int idx);

    std::string getPathRawData() {
        return pathRawData;
    }

    int *getOrder(int idx);

    uint64_t getInputSize() const {
        return inputSize;
    }

    uint64_t getNTerms() const {
        return nTerms;
    }

    void resetCounters() {
        strat.resetCounters();
        aggrIndices = notAggrIndices = cacheIndices = 0;
        spo = ops = pos = sop = osp = pso = 0;
    }

    Counters getCounters() {
        Counters c;
        c.statsRow = strat.statsRow;
        c.statsColumn = strat.statsColumn;
        c.statsCluster = strat.statsCluster;
        c.aggrIndices = aggrIndices;
        c.notAggrIndices = notAggrIndices;
        c.cacheIndices = cacheIndices;
        c.spo = spo;
        c.ops = ops;
        c.pos = pos;
        c.sop = sop;
        c.osp = osp;
        c.pso = pso;
        return c;
    }

    ArrayItr *getArrayIterator();

    PairItr *getPairIterator(TermCoordinates *value, int perm, long c1, long c2);

    PairItr *getFilePairIterator(const int perm, const long constraint1,
                                 const char strategy, const short file, const int pos);

    void releaseItr(PairItr *itr);

    DictMgmt *getDictMgmt() {
        return dict;
    }
};

#endif /* QUERIER_H_ */
