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

#ifndef BYTESTRACKER_H_
#define BYTESTRACKER_H_

#include <trident/kb/consts.h>

#include <iostream>
#include <boost/log/trivial.hpp>

using namespace std;

template<class K>
struct MemoryBlock {
    K *block;
    K** parentBlock;
    long bytes;
    int idx;
    int lock;
};

template<class K>
class MemoryManager {
private:
    const long cacheMaxSize;
    long bytes;

    MemoryBlock<K> **blocks;
    int start;
    int end;
    int blocksLeft;

    void removeOneBlock() {
        if (blocksLeft < MAX_N_BLOCKS_IN_CACHE) {
            if (start == MAX_N_BLOCKS_IN_CACHE) {
                start = 0;
            }

            //Check the block is not NULL
            while (blocks[start] == NULL || blocks[start]->lock > 0) {
                start++;
                if (start == MAX_N_BLOCKS_IN_CACHE) {
                    start = 0;
                }
            }
            removeBlock(start++);
        }
    }

public:
    MemoryManager(long cacheMaxSize) :
        cacheMaxSize(cacheMaxSize) {
        bytes = 0;
        blocks = new MemoryBlock<K>*[MAX_N_BLOCKS_IN_CACHE];
        memset(blocks, 0, sizeof(MemoryBlock<K>*) * MAX_N_BLOCKS_IN_CACHE);
        start = end = 0;
        blocksLeft = MAX_N_BLOCKS_IN_CACHE;
    }

    void update(int idx, long bytes) {
        this->bytes -= blocks[idx]->bytes;
        this->bytes += bytes;
        blocks[idx]->bytes = bytes;
    }

    void removeBlock(int idx) {
        //Delete block. This will trigger the deconstructor of K which should remove the block and update all the datastructures
        bytes -= blocks[idx]->bytes;
        K *elToRemove = blocks[idx]->block;
        blocks[idx]->parentBlock[blocks[idx]->idx] = NULL;
        delete blocks[idx];
        blocks[idx] = NULL;
        blocksLeft++;
        delete elToRemove;
    }

    void removeBlockWithoutDeallocation(int idx) {
        if (blocks[idx] != NULL) {
            bytes -= blocks[idx]->bytes;
            delete blocks[idx];
            blocks[idx] = NULL;
            blocksLeft++;
        }
    }

    void addLock(int idx) {
        blocks[idx]->lock++;
    }

    bool isUsed(int idx) {
        return blocks[idx]->lock > 0;
    }

    void releaseLock(int idx) {
        blocks[idx]->lock--;
    }

    int add(long bytes, K *element, int idxInParentArray, K **parentArray) {
        this->bytes += bytes;
        while (this->bytes >= cacheMaxSize || blocksLeft == 0) {
            removeOneBlock();
        }

        if (end == MAX_N_BLOCKS_IN_CACHE) {
            end = 0;
        }
        while (blocks[end] != NULL) {
            end = (end + 1) % MAX_N_BLOCKS_IN_CACHE;
        }

        MemoryBlock<K> *memoryBlock = new MemoryBlock<K>();
        memoryBlock->bytes = bytes;
        memoryBlock->block = element;
        memoryBlock->idx = idxInParentArray;
        memoryBlock->parentBlock = parentArray;
        memoryBlock->lock = 0;
        blocks[end] = memoryBlock;
        blocksLeft--;
        return end++;
    }

    ~MemoryManager() {
        while (blocksLeft < MAX_N_BLOCKS_IN_CACHE) {
            removeOneBlock();
        }
        delete[] blocks;
    }
};

#endif /* BYTESTRACKER_H_ */
