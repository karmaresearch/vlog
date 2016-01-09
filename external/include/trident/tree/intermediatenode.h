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

#ifndef INTERMEDIATENODE_H_
#define INTERMEDIATENODE_H_

#include <trident/tree/node.h>
#include <trident/kb/consts.h>

class IntermediateNode: public Node {
private:
    Node **children;
    long *idChildren;
    int lastUpdatedChild;

    Node *updateChildren(Node *split, int p,
                         void (*insertAverage)(Node*, int p, Node*, Node*));
    void ensureChildIsLoaded(int p);

public:

    IntermediateNode(TreeContext *context) :
        Node(context) {
        children = NULL;
        idChildren = NULL;
        lastUpdatedChild = -1;
    }

    IntermediateNode(TreeContext *context, Node *child1, Node *child2);

    bool get(nTerm key, long &coordinates);

    bool get(tTerm *key, const int sizeKey, nTerm *value);

    bool get(nTerm key, TermCoordinates *value);

    Node *put(nTerm key, long coordinatesTerm);

    Node *put(tTerm *key, int sizeKey, nTerm value);

    Node *put(nTerm key, TermCoordinates *value);

    Node *putOrGet(tTerm *key, int sizeKey, nTerm &value,
                   bool &insertResult);

    int unserialize(char *bytes, int pos);

    int serialize(char *bytes, int pos);

    bool canHaveChildren() {
        return true;
    }

    long smallestNumericKey();

    long largestNumericKey();

    tTerm *smallestTextualKey(int *size);

    tTerm *largestTextualKey(int *size);

    Node *getChildForKey(long key);

    Node *getChildForKey(tTerm *key, const int sizeKey);

    Node *getChildAtPos(int pos);

    int getPosChild(Node *child);

    void cacheChild(Node* child);

    Node *getChild(const int p);

    ~IntermediateNode();
};

#endif /* INTERMEDIATENODE_H_ */
