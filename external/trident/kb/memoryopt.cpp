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

#include <trident/kb/memoryopt.h>
#include <trident/kb/consts.h>
#include <trident/tree/leaf.h>

#include <tridentcompr/utils/utils.h>

#include <algorithm>

int MemoryOptimizer::calculateBytesPerDictPair(long nTerms, int leafSize) {
    int nbytesKey = 0;
    long tmp = nTerms;
    do {
        tmp >>= 8;
        nbytesKey++;
    } while (tmp > 0);

    //Know I know how many bytes I need for the key. What about the value?
    tmp = nTerms * 20;
    int nbytesValue = 0;
    do {
        tmp >>= 8;
        nbytesValue++;
    } while (tmp > 0);
    return (1 + nbytesKey + nbytesValue) * leafSize;
}

void MemoryOptimizer::optimizeForWriting(long inputTriples, KBConfig &config) {
    long totalMemory = (long) (Utils::getSystemMemory() * 0.8);
    long nTerms = inputTriples / 4;
    int dictionaries = config.getParamInt(DICTPARTITIONS);

    //Optimize tree with coordinates
    config.setParamInt(TREE_NODEMINBYTES, 1);
    config.setParamInt(TREE_MAXLEAVESCACHE, 2);

    int nNodesInCache = min(config.getParamInt(TREE_MAXELEMENTSNODE) + 1,
                            (int) (2 * nTerms / config.getParamInt(TREE_MAXELEMENTSNODE)) + 1);
    config.setParamInt(TREE_MAXNODESINCACHE, nNodesInCache);
    config.setParamInt(TREE_MAXPREALLLEAVESCACHE,
                       config.getParamInt(TREE_MAXNODESINCACHE) + 10);
    config.setParamLong(TREE_MAXSIZECACHETREE,
                        config.getParamInt(TREE_MAXFILESIZE) * 2);

    //Optimize size of leaves
    config.setParamInt(TREE_MAXPREALLINTERNALLINES,
                       config.getParamInt(TREE_MAXNODESINCACHE)
                       * config.getParamInt(TREE_MAXELEMENTSNODE) * 3);
    config.setParamInt(TREE_MAXINTERNALLINES,
                       config.getParamInt(TREE_MAXELEMENTSNODE) * 9);
    config.setParamInt(TREE_FACTORYSIZE, 50);
    config.setParamInt(TREE_ALLOCATEDELEMENTS,
                       config.getParamInt(TREE_MAXNODESINCACHE) + 10);

    //Keys used in the nodes of the tree
    config.setParamInt(TREE_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE,
                       config.getParamInt(TREE_MAXNODESINCACHE) + 10);

    //Optimize dictionaries trees
    int elementsInLeaf = config.getParamInt(DICT_MAXELEMENTSNODE);
    config.setParamInt(DICT_NODEMINBYTES,
                       calculateBytesPerDictPair(nTerms, elementsInLeaf));
//      config.setParamInt(DICT_NODEMINBYTES,1);

//  int bytesLeaf = sizeof(Leaf) + 16 * elementsInLeaf;
//  int maxNLeaves = ((long) (totalMemory / 3 / dictionaries)) / bytesLeaf;
//  config.setParamInt(DICT_MAXNODESINCACHE, maxNLeaves);
//  config.setParamInt(DICT_MAXLEAVESCACHE, 2);
//  long preallocatedLeaves = 10
//          + min(maxNLeaves, (int) (nTerms / dictionaries / elementsInLeaf));
//  config.setParamInt(DICT_MAXPREALLLEAVESCACHE, preallocatedLeaves);
//  config.setParamLong(DICT_MAXSIZECACHETREE,
//          (long) (totalMemory / 3 / dictionaries));
//  config.setParamInt(DICT_NODE_KEYS_FACTORY_SIZE, 10);
//  config.setParamInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE,
//          config.getParamInt(DICT_MAXPREALLLEAVESCACHE));

    config.setParamInt(DICT_MAXNODESINCACHE, 10000);
    config.setParamInt(DICT_MAXLEAVESCACHE, 2);
    config.setParamInt(DICT_MAXPREALLLEAVESCACHE,
                       config.getParamInt(DICT_MAXNODESINCACHE));
    config.setParamLong(DICT_MAXSIZECACHETREE,
                        (long) (totalMemory / 3 / dictionaries));
    config.setParamInt(DICT_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE,
                       config.getParamInt(DICT_MAXPREALLLEAVESCACHE));

    //Optimize string buffer
//  long estimatedTotalTermSize = 70 * nTerms;
//  int estimatedBlocks = max(
//          (int) (estimatedTotalTermSize / SB_BLOCK_SIZE / dictionaries), 1);
    int maxBlocks = max((int) (totalMemory / 3 / SB_BLOCK_SIZE / dictionaries),
                        1);
//  int preallocatedBlocks = min(maxBlocks, estimatedBlocks);
//  config.setParamInt(SB_PREALLBUFFERS, preallocatedBlocks);
    config.setParamInt(SB_PREALLBUFFERS, 1000);
    long cacheSize = (long) maxBlocks * SB_BLOCK_SIZE;
    config.setParamLong(SB_CACHESIZE, cacheSize);

    //Optimize inverse dictionary
    config.setParamInt(INVDICT_NODEMINBYTES, 1);
    config.setParamInt(INVDICT_MAXNODESINCACHE,
                       config.getParamInt(TREE_MAXNODESINCACHE));
    config.setParamLong(INVDICT_MAXSIZECACHETREE,
                        config.getParamLong(TREE_MAXSIZECACHETREE));
    config.setParamInt(INVDICT_MAXLEAVESCACHE, 2);
    config.setParamInt(INVDICT_MAXPREALLLEAVESCACHE,
                       config.getParamInt(INVDICT_MAXNODESINCACHE) + 10);
    config.setParamInt(INVDICT_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE,
                       config.getParamInt(INVDICT_MAXNODESINCACHE) + 10);

    //Optimize storage
//  config.setParamInt(STORAGE_MAX_FILE_SIZE, 1024 * 1024);

//This parameter does not count. The only thing that matters are the opened files...
    config.setParamLong(STORAGE_CACHE_SIZE,
                        (long) config.getParamInt(STORAGE_MAX_FILE_SIZE) * 3);
    config.setParamLong(STORAGE_MAX_N_FILES, 4);
}

void MemoryOptimizer::optimizeForReasoning(int ndicts, KBConfig &config) {
    long totalMemory = (uint64_t) std::min((double)128000000, (double)(Utils::getSystemMemory() * 0.10));

    //All memory (10% of total memory) is reserved for the secondary lists. Strings and Tree gets minimal memory.

    //StringBuffer
    config.setParamInt(SB_PREALLBUFFERS, 1);
    config.setParamLong(SB_CACHESIZE, 32 * 1024 * 1024);

    //Inv dict
    config.setParamLong(INVDICT_MAXSIZECACHETREE, 32 * 1024 * 1024);
    config.setParamInt(INVDICT_MAXNODESINCACHE, 100);
    config.setParamInt(INVDICT_MAXPREALLLEAVESCACHE, 100);
    config.setParamInt(INVDICT_MAXLEAVESCACHE, 10);
    config.setParamInt(INVDICT_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE, 1);

    //Dict
    config.setParamLong(DICT_MAXSIZECACHETREE, 32 * 1024 * 1024);
    config.setParamInt(DICT_MAXNODESINCACHE, 100);
    config.setParamInt(DICT_MAXPREALLLEAVESCACHE, 10);
    config.setParamInt(DICT_MAXLEAVESCACHE, 1);
    config.setParamInt(DICT_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE, 1);

    //Tree
    int nPreallocatedLeaves = 10;
    config.setParamLong(TREE_MAXSIZECACHETREE, 32 * 1024 * 1024);
    config.setParamInt(TREE_MAXNODESINCACHE, nPreallocatedLeaves);
    config.setParamInt(TREE_MAXPREALLLEAVESCACHE, nPreallocatedLeaves);
    config.setParamInt(TREE_MAXLEAVESCACHE, 10);

    //Leaves in the tree
    config.setParamInt(TREE_MAXPREALLINTERNALLINES, nPreallocatedLeaves * 1);
    config.setParamInt(TREE_MAXINTERNALLINES, 100);
    config.setParamInt(TREE_FACTORYSIZE, 10);
    config.setParamInt(TREE_ALLOCATEDELEMENTS, 10);

    //Nodes in the tree
    config.setParamInt(TREE_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE, 1);

    //Storage
    config.setParamLong(STORAGE_CACHE_SIZE, totalMemory);
    config.setParamLong(STORAGE_MAX_N_FILES, MAX_N_FILES);

}

void MemoryOptimizer::optimizeForReading(int ndicts, KBConfig &config) {
    long totalMemory = (long) (Utils::getSystemMemory() * 0.75);

    //Half of the memory is used to store the list of pairs. That's why we set it to totalMemory/2 (it's one shared among the three permutations).
    //The other half is divided among the tree (2/3) and the inv dicts (1/3).

    //StringBuffer
    config.setParamInt(SB_PREALLBUFFERS, 100);
    config.setParamLong(SB_CACHESIZE, totalMemory / 6 / ndicts);

    //Inv dict
    config.setParamLong(INVDICT_MAXSIZECACHETREE, 1024 * 1024 * 1024);
    config.setParamInt(INVDICT_MAXNODESINCACHE, 10000);
    config.setParamInt(INVDICT_MAXPREALLLEAVESCACHE, 1000);
    config.setParamInt(INVDICT_MAXLEAVESCACHE, 10);
    config.setParamInt(INVDICT_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE, 1);

    //Dict
    config.setParamLong(DICT_MAXSIZECACHETREE,
                        config.getParamInt(DICT_MAXFILESIZE));
    config.setParamInt(DICT_MAXNODESINCACHE, 1000);
    config.setParamInt(DICT_MAXPREALLLEAVESCACHE, 10);
    config.setParamInt(DICT_MAXLEAVESCACHE, 1);
    config.setParamInt(DICT_NODE_KEYS_FACTORY_SIZE, 100);
    config.setParamInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE, 1);

    //Tree
    int nPreallocatedLeaves = 10000;
    long sizePreallocatedLeaves = sizeof(Leaf) * nPreallocatedLeaves;
    config.setParamLong(TREE_MAXSIZECACHETREE,
                        ((totalMemory / 6) * 2) - sizePreallocatedLeaves);
    config.setParamInt(TREE_MAXNODESINCACHE, nPreallocatedLeaves);
    config.setParamInt(TREE_MAXPREALLLEAVESCACHE, nPreallocatedLeaves);
    config.setParamInt(TREE_MAXLEAVESCACHE, 10);

    //Leaves in the tree
    config.setParamInt(TREE_MAXPREALLINTERNALLINES, nPreallocatedLeaves * 100);
    config.setParamInt(TREE_MAXINTERNALLINES, 10000);
    config.setParamInt(TREE_FACTORYSIZE, 10);
    config.setParamInt(TREE_ALLOCATEDELEMENTS, 10);

    //Nodes in the tree
    config.setParamInt(TREE_NODE_KEYS_FACTORY_SIZE, 10);
    config.setParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE, 1);

    //Storage
    config.setParamLong(STORAGE_CACHE_SIZE, totalMemory / 2);
    config.setParamLong(STORAGE_MAX_N_FILES, MAX_N_FILES);
}
