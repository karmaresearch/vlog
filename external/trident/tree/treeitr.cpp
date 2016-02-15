/*
 * treeitr.cpp
 *
 *  Created on: Oct 13, 2013
 *      Author: jacopo
 */

#include <trident/tree/treeitr.h>
#include <trident/tree/leaf.h>
#include <trident/tree/treecontext.h>
#include <trident/tree/cache.h>

TreeItr::TreeItr(Node *root, Leaf *firstLeaf) {
    this->root = root;
    currentLeaf = firstLeaf;
    currentPos = 0;

    //I temporary remove this leaf from the caching system.
    cache = root->getContext()->getCache();
//  cache->unregisterNode(currentLeaf);
}

bool TreeItr::hasNext() {
    if (currentPos >= currentLeaf->getCurrentSize()) {

        Node *n = currentLeaf->getRightSibling();

        if (n == currentLeaf) {
//          cache->registerNode(currentLeaf);
            return false;
        }

//      cache->registerNode(currentLeaf);
        currentLeaf = (Leaf *) n;
//      cache->unregisterNode(currentLeaf);

        currentPos = 0;
    }
    return true;
}

long TreeItr::next(TermCoordinates *value) {
    currentLeaf->getValueAtPos(currentPos, value);
    return currentLeaf->getKey(currentPos++);
}

long TreeItr::next(long &value) {
    nTerm v;
    currentLeaf->getValueAtPos(currentPos, &v);
    value = v;
    return currentLeaf->getKey(currentPos++);
}
