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

#ifndef _CACHEITR
#define _CACHEITR

#include <trident/iterators/pairitr.h>

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

    bool hasNextChecked, next;

    CacheBlock *searchBlock(std::vector<CacheBlock> *blocks,
                            const uint64_t v);
public:
    int getType()  {
        return 5;
    }

    void init(Querier *q, const uint64_t estimatedSize, CacheIdx *cache, long c1,
              long c2);

    long getValue1();

    long getValue2();

    bool has_next();

    void next_pair();

    void clear();

    void move_first_term(long c1);

    void move_second_term(long c2);

    void mark();

    uint64_t getCard();

    void reset(const char i);

    bool allowMerge() {
        return false;
    }
};

#endif
