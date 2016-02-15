#ifndef _CACHEIDX
#define _CACHEIDX

#include <google/dense_hash_map>
#include <vector>
#include <cstdint>
#include <climits>

struct CacheBlock {
    uint64_t startKey;
    uint64_t endKey;
    uint64_t startArray;
    uint64_t endArray;

    bool operator < (const CacheBlock &b1) const {
        if (startKey == b1.startKey) {
            return endKey > b1.endKey;
        }
        return startKey < b1.startKey;
    }
};

struct eqnumbers {
    bool operator()(const uint64_t v1, const uint64_t v2) const {
        return v1 == v2;
    }
};


typedef google::dense_hash_map<uint64_t,
        std::pair<std::vector<CacheBlock>*, std::vector<std::pair<uint64_t, uint64_t>>*>,
        std::hash<uint64_t>, eqnumbers> KeyMap;

class CacheIdx {
private:
    KeyMap keyMap;
public:

    CacheIdx() {
        keyMap.set_empty_key(~0lu);
    }

    std::pair<std::vector<std::pair<uint64_t, uint64_t>>*,
        std::vector<CacheBlock>*
        > getIndex(uint64_t key);

    void storeIdx(uint64_t key, std::vector<CacheBlock> *blocks,
                  std::vector<std::pair<uint64_t, uint64_t>> *pairs);

    ~CacheIdx();
};

#endif
