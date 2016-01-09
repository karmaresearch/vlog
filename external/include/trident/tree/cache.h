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

#ifndef CACHE_H_
#define CACHE_H_

#include <trident/tree/nodemanager.h>
#include <trident/tree/node.h>
#include <trident/tree/leaffactory.h>
#include <trident/kb/consts.h>

#include <boost/chrono.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/log/trivial.hpp>

#include <list>
#include <string>

class Node;
class TreeContext;

namespace timens = boost::chrono;

class Cache {

private:
    TreeContext *context;

    const bool compressedNodes;

    boost::circular_buffer<Node*> registeredNodes;

    NodeManager *manager;

    char supportBuffer[SIZE_SUPPORT_BUFFER];
    char supportBuffer2[SIZE_SUPPORT_BUFFER];

    LeafFactory *factory;
public:

    Cache(int maxNodesInCache, bool compressedNodes) :
        compressedNodes(compressedNodes), registeredNodes(maxNodesInCache) {
//      BOOST_LOG_TRIVIAL(debug)<< "Init cache: maxNodesInCache=" << maxNodesInCache << " compressed? " << compressedNodes;
        context = NULL;
        factory = NULL;
        manager = NULL;
    }

    void init(TreeContext *context, std::string path, int fileMaxSize,
              int maxNFiles, long cacheMaxSize, int sizeLeavesFactory,
              int sizePreallLeavesFactory, int nodeMinBytes);

    Node *getNodeFromCache(long id);

    void registerNode(Node *node);

    Leaf *newLeaf() {
        return factory->get();
    }

    IntermediateNode *newIntermediateNode();

    IntermediateNode *newIntermediateNode(Node *child1, Node *child2);

    void flushNode(Node *node, const bool registerNode);

    void flushAllCache();

    ~Cache() {
        Node *node = NULL;
        while (!registeredNodes.empty()) {
            node = registeredNodes.front();
            registeredNodes.pop_front();
            if (node->shouldDeallocate())
                delete node;
        }
        registeredNodes.clear();

        delete factory;
        delete manager;
    }
};

#endif /* CACHE_H_ */
