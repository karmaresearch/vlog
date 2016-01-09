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
