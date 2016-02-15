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
