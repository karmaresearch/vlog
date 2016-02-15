#ifndef _CACHEITR
#define _CACHEITR

#include <trident/iterators/cacheitr.h>
#include <trident/iterators/pairitr.h>
#include <trident/kb/consts.h>

#include <trident/kb/cacheidx.h>

class CacheIdx;
class Querier;

class CacheItr : public PairItr {
private:
    Querier *q;
    uint64_t estimatedSize;
    CacheIdx *cache;

    std::vector<std::pair<uint64_t, uint64_t>> newPairs;
    std::vector<CacheBlock> newBlocks;

    std::vector<std::pair<uint64_t, uint64_t>> *p;
    uint64_t idxEndGroup;

    std::vector<std::pair<uint64_t, uint64_t>> *existingPairs;
    std::vector<CacheBlock> *existingBlocks;

    uint64_t currentIdx, v1, v2;
    long lastDeltaValue, startDelta;
    bool groupSet;

    bool hasNextChecked, n;

    CacheBlock *searchBlock(std::vector<CacheBlock> *blocks,
                            const uint64_t v);
public:
    int getTypeItr()  {
        return CACHE_ITR;
    }

    void init(Querier *q, const uint64_t estimatedSize, CacheIdx *cache, long c1,
              long c2);

    long getValue1();

    long getValue2();

    bool hasNext();

    void next();

    void clear();

    void gotoFirstTerm(long c1);

    void gotoSecondTerm(long c2);

    void mark();

    uint64_t getCardinality();

    //uint64_t estimateCardinality();

    void reset(const char i);

    bool allowMerge() {
        return false;
    }

    void ignoreSecondColumn() {
        throw 10; //not supported
    }
};

#endif
