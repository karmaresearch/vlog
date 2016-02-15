/*
 * intermediatenode.h
 *
 *  Created on: Oct 6, 2013
 *      Author: jacopo
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
