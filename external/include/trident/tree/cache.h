/*
 * cache.h
 *
 *  Created on: Oct 6, 2013
 *      Author: jacopo
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
