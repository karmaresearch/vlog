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

#include <trident/tree/intermediatenode.h>
#include <trident/tree/treecontext.h>
#include <trident/tree/cache.h>

#include <tridentcompr/utils/utils.h>

#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>

#include <iostream>
#include <assert.h>

#define CHILD_NOT_FOUND -1

IntermediateNode::IntermediateNode(TreeContext *context, Node *child1,
                                   Node *child2) :
    Node(context) {
    children = new Node*[context->getMaxElementsPerNode() + 1];
    idChildren = new long[context->getMaxElementsPerNode() + 1];

    if (context->textKeys()) {
        int size = 0;
        tTerm *key = child1->largestTextualKey(&size);
        putkeyAt(key, size, 0);
    } else {
        long key1 = child1->largestNumericKey();
        long key2 = child2->smallestNumericKey();
        putkeyAt((key1 + key2) / 2, 0);
    }
    children[0] = child1;
    idChildren[0] = child1->getId();
    children[1] = child2;
    idChildren[1] = child2->getId();
    lastUpdatedChild = -1;
    setState(STATE_MODIFIED);
}

int IntermediateNode::unserialize(char *bytes, int pos) {
    pos = Node::unserialize(bytes, pos);

    if (children == NULL) {
        children = new Node*[getCurrentSize() + 1];
        idChildren = new long[getCurrentSize() + 1];
    }

    for (int i = 0; i < getCurrentSize() + 1; ++i) {
        long id = Utils::decode_long(bytes, pos);
        pos += 8;
        idChildren[i] = id;
        children[i] = NULL;
    }
    return pos;
}

int IntermediateNode::serialize(char *bytes, int pos) {
    pos = Node::serialize(bytes, pos);

    for (int i = 0; i < getCurrentSize() + 1; ++i) {
        long id;
        if (children[i] != NULL) {
            id = children[i]->getId();
        } else {
            id = idChildren[i];
        }
        Utils::encode_long(bytes, pos, id);
        pos += 8;
    }
    return pos;
}

Node *IntermediateNode::getChildForKey(long key) {
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
    }
    ensureChildIsLoaded(p);
    return children[p];
}

Node *IntermediateNode::getChildAtPos(int p) {
    ensureChildIsLoaded(p);
    return children[p];
}

int IntermediateNode::getPosChild(Node *child) {
    if (lastUpdatedChild < getCurrentSize()
            && children[lastUpdatedChild + 1] == child) {
        return lastUpdatedChild + 1;
    }

    int p = 0;
    if (!getContext()->textKeys()) {
        p = pos(child->smallestNumericKey());
    } else {
        int size = 0;
        tTerm *t = child->smallestTextualKey(&size);
        p = pos(t, size);
    }
    if (p < 0) {
        p = -p - 1;
    }
    if (children[p] == child) {
        return p;
    }

    return CHILD_NOT_FOUND;
}

long IntermediateNode::smallestNumericKey() {
    ensureChildIsLoaded(0);
    return children[0]->smallestNumericKey();
}

long IntermediateNode::largestNumericKey() {
    ensureChildIsLoaded(getCurrentSize());
    return children[getCurrentSize()]->largestNumericKey();
}

tTerm *IntermediateNode::smallestTextualKey(int *size) {
    ensureChildIsLoaded(0);
    return children[0]->smallestTextualKey(size);
}

tTerm *IntermediateNode::largestTextualKey(int *size) {
    ensureChildIsLoaded(getCurrentSize());
    return children[getCurrentSize()]->largestTextualKey(size);
}

void IntermediateNode::cacheChild(Node *child) {
    int p = getPosChild(child);
    if (p == CHILD_NOT_FOUND) {
        BOOST_LOG_TRIVIAL(error) << "Child: " << child->getId() << " is not found on node " << getId();
        BOOST_LOG_TRIVIAL(error) << "CacheChild(): Position not found!";
    }
    children[p] = NULL;
    idChildren[p] = child->getId();
    lastUpdatedChild = p;
}

bool IntermediateNode::get(tTerm *key, const int sizeKey, nTerm *value) {
    int p = pos(key, sizeKey);
    if (p < 0) {
        p = -p - 1;
    }
    ensureChildIsLoaded(p);
    return children[p]->get(key, sizeKey, value);
}

//bool IntermediateNode::get(nTerm key, tTerm *container) {
//  int p = pos(key);
//  if (p < 0) {
//      p = -p - 1;
//  }
//  ensureChildIsLoaded(p);
//  return children[p]->get(key, container);
//}
bool IntermediateNode::get(nTerm key, long &coordinates) {
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
    }
    ensureChildIsLoaded(p);
    return children[p]->get(key, coordinates);
}

bool IntermediateNode::get(nTerm key, TermCoordinates *value) {
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
    }
    ensureChildIsLoaded(p);
    return children[p]->get(key, value);
}

void numAvg(Node *parent, int p, Node *child1, Node *child2) {
    long key1 = child1->largestNumericKey();
    long key2 = child2->smallestNumericKey();
    parent->putkeyAt((key1 + key2) / 2, p);
}

void textAvg(Node *parent, int p, Node *child1, Node *child2) {
    int size = 0;
    tTerm *key = child1->largestTextualKey(&size);
    parent->putkeyAt(key, size, p);
}

Node *IntermediateNode::updateChildren(Node *split, int p,
                                       void (*insertAverage)(Node*, int p, Node*, Node*)) {
    setState(STATE_MODIFIED);
    if (shouldSplit()) {
        IntermediateNode *n1 = getContext()->getCache()->newIntermediateNode();
        n1->setId(getContext()->getNewNodeID());
        n1->setState(STATE_MODIFIED);

        if (n1->children == NULL) {
            n1->children = new Node*[getContext()->getMaxElementsPerNode() + 1];
            n1->idChildren =
                new long[getContext()->getMaxElementsPerNode() + 1];
        }

        int minSize = getContext()->getMinElementsPerNode();
        if (p < minSize) {
            for (int i = 0; i < minSize + 1; ++i) {
                n1->children[i] = children[minSize + i];
                if (n1->children[i] != NULL) {
                    n1->children[i]->setParent(n1);
                } else {
                    n1->idChildren[i] = idChildren[minSize + i];
                }
                children[minSize + i] = NULL;
            }
            this->split(n1);
            removeLastKey();
            insertAverage(this, p, children[p], split);

            memmove(children + p + 2, children + p + 1,
                    (getCurrentSize() - p - 1) * sizeof(Node *));
            memmove(idChildren + p + 2, idChildren + p + 1,
                    (getCurrentSize() - p - 1) * sizeof(long));
            children[p + 1] = split;
            split->setParent(this);

        } else {
            for (int i = 0; i < minSize; ++i) {
                n1->children[i] = children[minSize + 1 + i];
                if (n1->children[i] != NULL) {
                    n1->children[i]->setParent(n1);
                } else {
                    n1->idChildren[i] = idChildren[minSize + 1 + i];
                }
                children[minSize + 1 + i] = NULL;
            }
            this->split(n1);

            if (p > minSize) {
                n1->removeFirstKey();
                p -= minSize + 1;
                memmove(n1->children + p + 2, n1->children + p + 1,
                        sizeof(Node *) * (minSize - p - 1));
                memmove(n1->idChildren + p + 2, n1->idChildren + p + 1,
                        sizeof(long) * (minSize - p - 1));

                insertAverage(n1, p, n1->children[p], split);
                n1->children[p + 1] = split;
            } else { // CASE: pos == getMinSize()
                memmove(n1->children + 1, n1->children,
                        minSize * sizeof(Node*));
                memmove(n1->idChildren + 1, n1->idChildren,
                        minSize * sizeof(long));
                n1->children[0] = split;
            }

            split->setParent(n1);
        }
        getContext()->getCache()->registerNode(split);
        return n1;
    } else {
        insertAverage(this, p, children[p], split);
        memmove(children + p + 2, children + p + 1,
                (getCurrentSize() - p - 1) * sizeof(Node*));
        memmove(idChildren + p + 2, idChildren + p + 1,
                (getCurrentSize() - p - 1) * sizeof(long));
        children[p + 1] = split;
        split->setParent(this);
        getContext()->getCache()->registerNode(split);
        return NULL;
    }
}

Node *IntermediateNode::put(nTerm key, long coordinatesTTerm) {
    // Forward the request to the children
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
    }

    ensureChildIsLoaded(p);
    Node *split = children[p]->put(key, coordinatesTTerm);
    if (split != NULL) {
        return updateChildren(split, p, &numAvg);
    } else {
        return NULL;
    }
}

Node *IntermediateNode::put(nTerm key, TermCoordinates *value) {
    // Forward the request to the children
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
    }

    ensureChildIsLoaded(p);

    Node *split = children[p]->put(key, value);
    if (split != NULL) {
        return updateChildren(split, p, &numAvg);
    } else {
        return NULL;
    }
}

Node *IntermediateNode::put(tTerm *key, int sizeKey, nTerm value) {
    // Forward the request to the children
    int p = pos(key, sizeKey);
    if (p < 0) {
        p = -p - 1;
    }

    ensureChildIsLoaded(p);

    Node *split = children[p]->put(key, sizeKey, value);
    if (split != NULL) {
        return updateChildren(split, p, &textAvg);
    } else {
        return NULL;
    }
}

Node *IntermediateNode::putOrGet(tTerm *key, int sizeKey, nTerm &value,
                                 bool &insertResult) {
    // Forward the request to the children
    int p = pos(key, sizeKey);
    if (p < 0) {
        p = -p - 1;
    }

    ensureChildIsLoaded(p);

    Node *split = children[p]->putOrGet(key, sizeKey, value, insertResult);
    if (insertResult && split != NULL) {
        return updateChildren(split, p, &textAvg);
    } else {
        return NULL;
    }
}

void IntermediateNode::ensureChildIsLoaded(int p) {
    if (children[p] == NULL) {
        children[p] = getContext()->getCache()->getNodeFromCache(idChildren[p]);
        children[p]->setParent(this);
        getContext()->getCache()->registerNode(children[p]);
    }
}

Node *IntermediateNode::getChild(const int p) {
    return children[p];
}

Node *IntermediateNode::getChildForKey(tTerm *key, const int sizeKey) {
    int p = pos(key, sizeKey);
    if (p < 0) {
        p = -p - 1;
    }
    ensureChildIsLoaded(p);
    return children[p];
}

IntermediateNode::~IntermediateNode() {
    if (idChildren != NULL)
        delete[] idChildren;

    if (children != NULL) {
        if (getContext()->isReadOnly()) {
            for (int i = 0; i < getCurrentSize() + 1; ++i) {
                if (children[i] != NULL && children[i]->canHaveChildren()) {
                    delete children[i];
                }
            }
        }
        delete[] children;
    }
}

