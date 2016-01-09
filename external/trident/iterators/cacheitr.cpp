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

#include <trident/iterators/cacheitr.h>
#include <trident/kb/querier.h>
#include <trident/kb/cacheidx.h>

void CacheItr::init(Querier *q, const uint64_t estimatedSize, CacheIdx *cache,
                    long c1, long c2) {
    this->cache = cache;
    this->estimatedSize = estimatedSize;
    this->q = q;
    constraint1 = c1;
    constraint2 = c2;

    v1 = 0;
    v2 = 0;

    idxEndGroup = 0;
    currentIdx = 0;
    groupSet = false;
    newPairs.clear();
    p = NULL;

    newBlocks.clear();
    startDelta = 0;
    lastDeltaValue = -2;

    //Initially the iterator is set to return some triples since there must
    //be some. The first time that the iterator is moved to a location then
    //the flags will be set accordingly.
    hasNextChecked = true;
    next = true;

    //Get existing pairs from cache
    std::pair<std::vector<std::pair<uint64_t, uint64_t>>*, std::vector<CacheBlock>*> pair =
                cache->getIndex(getKey());
    existingPairs = pair.first;
    existingBlocks = pair.second;
}

long CacheItr::getValue1() {
    return v1;
}

long CacheItr::getValue2() {
    return v2;
}

bool CacheItr::has_next() {
    if (!hasNextChecked) {
        next = true;
        if (groupSet) {
            if (currentIdx >= idxEndGroup || p->at(currentIdx).first != v1) {
                next = false;
            } else if (constraint2 != -1 && p->at(currentIdx).second != constraint2) {
                next = false;
            }
        } else {
            next = false;
        }
        hasNextChecked = true;
    }
    assert(groupSet || !next);
    return next;
}

void CacheItr::next_pair() {
    assert(groupSet);
    v2 = p->at(currentIdx).second;
    currentIdx++;
    hasNextChecked = false;
}

void CacheItr::clear() {
    BOOST_LOG_TRIVIAL(debug) << "Collected " << newPairs.size() << " pairs and " << newBlocks.size() << " blocks. All pairs are " << estimatedSize;

    if (newPairs.size() > 0) {
        if (startDelta != newPairs.size()) {
            CacheBlock b;
            b.startKey = newPairs[startDelta].first;
            b.endKey = newPairs[newPairs.size() - 1].first;
            b.startArray = startDelta;
            b.endArray = newPairs.size();
            newBlocks.push_back(b);
        }

        std::sort(newBlocks.begin(), newBlocks.end());
        //Remove the duplicates
        uint64_t lastKey = 0;
        uint64_t lastEndKey = 0;
        bool first = true;
        std::vector<CacheBlock> *cacheBlock = new std::vector<CacheBlock>();
        for (std::vector<CacheBlock>::iterator itr = newBlocks.begin();
                itr != newBlocks.end(); ++itr) {
            if (first) {
                cacheBlock->push_back(*itr);
                first = false;
                lastKey = itr->startKey;
                lastEndKey = itr->endKey;
            } else if (itr->startKey != lastKey) {
                if (itr->startKey <= lastEndKey) {
                    if (itr->endKey <= lastEndKey) {
                        //Continue
                        continue;
                    } else {
                        //Change the key, and move forward
                        itr->startArray += lastEndKey - itr->startKey + 1;
                        itr->startKey = lastEndKey + 1;
                    }
                }
                cacheBlock->push_back(*itr);
                lastKey = itr->startKey;
                lastEndKey = itr->endKey;
            }
        }

        std::vector<std::pair<uint64_t, uint64_t>> *cachePairs =
                new std::vector<std::pair<uint64_t, uint64_t>>();
        newPairs.swap(*cachePairs);
        cache->storeIdx(getKey(), cacheBlock, cachePairs);
        newPairs.clear();
        newBlocks.clear();
    }
}

CacheBlock *CacheItr::searchBlock(std::vector<CacheBlock> *blocks,
                                  const uint64_t v) {
    if (blocks == NULL)
        return NULL;

    CacheBlock b;
    b.startKey = v;
    std::vector<CacheBlock>::iterator itr = std::lower_bound(blocks->begin(),
                                            blocks->end(), b);
    if (itr != blocks->begin() || v == itr->startKey) {
        if (v != itr->startKey)
            itr--;
        if (v <= itr->endKey) {
            return &(*itr);
        } else {
            return NULL;
        }
    } else {
        return NULL;
    }
}

void CacheItr::move_first_term(long c1) {
    //does c1 exist in the existing blocks? For now, assume it does not
    CacheBlock *block = searchBlock(existingBlocks, c1);
    if (block != NULL) {
        p = existingPairs;
        currentIdx = block->startArray + c1 - block->startKey;
        idxEndGroup = block->endArray;
        assert(currentIdx < idxEndGroup);
        while (currentIdx < idxEndGroup && p->at(currentIdx).first < c1) {
            ++currentIdx;
        }
        v1 = c1;
        v2 = p->at(currentIdx).second;
        hasNextChecked = true;
        next = p->at(currentIdx).first == c1;
        groupSet = true;
        return;
    }

    if (c1 != lastDeltaValue + 1 && newPairs.size() != startDelta) {
        //Start a new delta...
        CacheBlock b;
        b.startKey = newPairs[startDelta].first;
        b.endKey = newPairs[newPairs.size() - 1].first;
        b.startArray = startDelta;
        b.endArray = newPairs.size();
        newBlocks.push_back(b);
        startDelta = newPairs.size();
    }

    currentIdx = newPairs.size();
    PairItr *subitr = q->get(IDX_SPO, c1, getKey(), -1);
    while (subitr->has_next()) {
        subitr->next_pair();
        newPairs.push_back(std::make_pair(c1, subitr->getValue2()));
    }
    q->releaseItr(subitr);

    if (newPairs.size() != currentIdx) {
        p = &newPairs;
        lastDeltaValue = c1;
        v1 = c1;
        v2 = newPairs.at(currentIdx).second;
        next = true;
        groupSet = true;
        idxEndGroup = newPairs.size();
    } else {
        next = false;
        groupSet = false;
    }
    hasNextChecked = true;
}

void CacheItr::move_second_term(long c2) {
    while (currentIdx < idxEndGroup && v1 == p->at(currentIdx).first &&
            p->at(currentIdx).second < c2) {
        currentIdx++;
    }
    if (currentIdx < idxEndGroup && v1 == p->at(currentIdx).first &&
            p->at(currentIdx).second == c2)
        next = true;
    else
        next = false;
    hasNextChecked = true;
}

void CacheItr::mark() {
}

void CacheItr::reset(const char i) {
}

uint64_t CacheItr::getCard() {
    return estimatedSize;
}
