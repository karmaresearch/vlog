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

#ifndef NODEMANAGER_H_
#define NODEMANAGER_H_

#include <trident/tree/treecontext.h>
#include <trident/files/filemanager.h>
#include <trident/files/filedescriptor.h>

#include <boost/unordered_map.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <string>

using namespace std;
namespace bip = boost::interprocess;

class TreeContext;

typedef struct CachedNode {
    long id;
    CachedNode *previous;
    CachedNode *next;
    int nodeSize;
    int availableSize;
    int posIndex;
    short fileIndex;
    bool children;
} CachedNode;

struct StoredNodesKeyHasher {
    std::size_t operator()(long n) const {
        return (int) n;
    }
};
struct StoredNodesKeyCmp {
    bool operator()(long o1, long o2) const {
        return o1 == o2;
    }
};

#define NODE_SIZE 25

class NodeManager {
private:
    const bool readOnly;
    const string path;
    const int nodeMinSize;

    //Used during the writing
    boost::unordered_map<long, CachedNode*, StoredNodesKeyHasher,
          StoredNodesKeyCmp> storedNodes;
    //Used in cache it's read-only
    CachedNode *readOnlyStoredNodes;
    bool *nodesLoaded;
    bip::file_mapping *mapping;
    bip::mapped_region *mapped_rgn;
    char *rawInput;

    std::vector<CachedNode*> firstElementsPerFile;
    FileManager<FileDescriptor, FileDescriptor> *manager;
    MemoryManager<FileDescriptor> *bytesTracker;

    short lastCreatedFile;
    CachedNode *lastNodeInserted;

    static void unserializeNodeFrom(CachedNode *node, char *buffer, int pos);

    static int serializeTo(CachedNode *node, char *buffer);

public:
    NodeManager(TreeContext *context, int nodeMinBytes, int fileMaxSize,
                int maxNFiles, long cacheMaxSize, std::string path);

    char* get(CachedNode *node);

    void put(Node *node, char *buffer, int sizeBuffer);

    CachedNode *getCachedNode(long id);

    static void compressSpace(string path);

    ~NodeManager();
};

#endif /* NODEMANAGER_H_ */
