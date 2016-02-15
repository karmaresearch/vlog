#ifndef QUERIER_H_
#define QUERIER_H_

//#include <trident/iterators/storageitr.h>
#include <trident/iterators/arrayitr.h>
#include <trident/iterators/scanitr.h>
#include <trident/iterators/simplescanitr.h>
#include <trident/iterators/aggritr.h>
#include <trident/iterators/cacheitr.h>
#include <trident/iterators/termitr.h>
#include <trident/tree/coordinates.h>
#include <trident/binarytables/storagestrat.h>

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
    const long *nTablesPerPartition;
    const long *nFirstTablesPerPartition;

    std::string pathRawData;
    bool copyRawData;

    const int nindices;
    //CacheIdx *cachePSO;

    std::unique_ptr<Querier> sampler;

    TermCoordinates currentValue;

    //Factories
    Factory<ArrayItr> factory2;
    Factory<ScanItr> factory3;
    Factory<AggrItr> factory4;
    Factory<CacheItr> factory5;
    Factory<SimpleScanItr> factory6;
    Factory<TermItr> factory7;

    Factory<RowTable> listFactory;
    Factory<ClusterTable> comprFactory;
    Factory<ColumnTable> list2Factory;
    Factory<NewColumnTable> ncFactory;

    StorageStrat strat;

    //Statistics
    long aggrIndices, notAggrIndices, cacheIndices;
    long spo, ops, pos, sop, osp, pso;

    void initBinaryTable(TableStorage *storage,
                         int fileIdx,
                         int mark,
                         BinaryTable *t,
                         long v1,
                         long v2,
                         const bool setConstraints);

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

    Querier(Root* tree, DictMgmt *dict, TableStorage** files,
            const long inputSize,
            const long nTerms, const int nindices,
            const long* nTablesPerPartition,
            const long* nFirstTablesPerPartition,
            KB *sampleKB);

    TermItr *getTermList(const int perm) {
        return getTermList(perm, false);
    }

    TermItr *getTermList(const int perm, const bool enforcePerm);

    TableStorage *getTableStorage(const int perm) {
        if (nindices <= perm)
            return NULL;
        else
            return files[perm];
    }

    StorageStrat *getStorageStrat() {
        return &strat;
    }

    PairItr *get(const int idx, const long s, const long p, const long o) {
        return get(idx, s, p, o, true);
    }

    PairItr *get(const int idx, const long s, const long p,
                 const long o, const bool cons);

    PairItr *get(const int idx, TermCoordinates &value,
                 const long key, const long v1,
                 const long v2, const bool cons);

    PairItr *get(const int perm,
                 const long key,
                 const short fileIdx,
                 const int mark,
                 const char strategy,
                 const long v1,
                 const long v2,
                 const bool constrain,
                 const bool noAggr);

    PairItr *getPermuted(const int idx, const long el1, const long el2,
                         const long el3, const bool constrain);

    uint64_t isAggregated(const int idx, const long first, const long second,
                   const long third);

    uint64_t isReverse(const int idx, const long first, const long second,
                   const long third);

    uint64_t getCardOnIndex(const int idx, const long first, const long second, const long third) {
        return getCardOnIndex(idx, first, second, third, false);
    }

    uint64_t getCardOnIndex(const int idx, const long first, const long second,
                            const long third, bool skipLast);

    long getCard(const long s, const long p, const long o);

    long getCard(const long s, const long p, const long o, uint8_t pos);

    uint64_t getCard(const int idx, const long v);

    uint64_t estCardOnIndex(const int idx, const long first, const long second,
                            const long third);

    long estCard(const long s, const long p, const long o);

    bool isEmpty(const long s, const long p, const long o);

    int getIndex(const long s, const long p, const long o);

    char getStrategy(const int idx, const long v);

    Querier &getSampler();

    int *getInvOrder(int idx);

    std::string getPathRawData() {
        return pathRawData;
    }

    int *getOrder(int idx);

    bool permExists(const int perm) const;

    uint64_t getInputSize() const {
        return inputSize;
    }

    /*uint64_t getNTablesPerPartition(const int idx) const {
        return nTablesPerPartition[idx];
    }*/

    uint64_t getNFirstTablesPerPartition(const int idx) const {
        return nFirstTablesPerPartition[idx];
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

    PairItr *getPairIterator(TermCoordinates *value,
                             int perm,
                             const long key,
                             long c1,
                             long c2,
                             const bool constrain,
                             const bool noAggr);

    PairItr *newItrOnReverse(PairItr *itr, const long v1, const long v2);

    PairItr *getFilePairIterator(const int perm, const long constraint1,
                                 const char strategy, const short file,
                                 const int pos);

    void releaseItr(PairItr *itr);

    DictMgmt *getDictMgmt() {
        return dict;
    }
};

#endif /* QUERIER_H_ */
