/*
 * treeitr.h
 *
 *  Created on: Oct 13, 2013
 *      Author: jacopo
 */

#ifndef TREEITR_H_
#define TREEITR_H_

#include <trident/kb/kb.h>

class TermCoordinates;
class Leaf;
class Node;
class Cache;

class TreeItr {
private:
    Node *root;
    Leaf *currentLeaf;
    int currentPos;
    Cache *cache;

public:
    TreeItr(Node *root, Leaf *firstLeaf);

    bool hasNext();

    long next(TermCoordinates *value);

    long next(long &value);
};

#endif /* TREEITR_H_ */
