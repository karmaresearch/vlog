/*
 * leaf.h
 *
 *  Created on: Oct 6, 2013
 *      Author: jacopo
 */

#ifndef LEAF_H_
#define LEAF_H_

#include <trident/tree/coordinates.h>
#include <trident/tree/node.h>
#include <trident/kb/consts.h>

#include <tridentcompr/utils/factory.h>
#include <tridentcompr/utils/utils.h>

#include <iostream>
#include <inttypes.h>

using namespace std;

#define MAX_INSTANTIED_POS 10

class Leaf: public Node {
private:
    char *rawNode;
    Coordinates **detailPermutations1;
    Coordinates **detailPermutations2;

    bool scanThroughArray;
    int nInstantiatedPositions;
    short instantiatedPositions[MAX_INSTANTIED_POS];

    //Numeric values
    nTerm *numValue;
    int sNumValue;

    void clearFirstLevel(int size);

    Coordinates *parseInternalLine(const int pos);

    Node *insertAtPosition(int p, tTerm *key, int sizeKey, nTerm value);

    Coordinates *addCoordinates(Coordinates* initial, TermCoordinates *value);

    /***** UNSERIALIZATION METHODS *****/
    int unserialize_numberlist(char *bytes, int pos);
    int unserialize_values(char *bytes, int pos, int previousSize);

    int serialize_numberlist(char *bytes, int pos);
    int serialize_values(char *bytes, int pos);

public:
    Leaf(TreeContext *context);

    Leaf(const Leaf& other);

    int unserialize(char *bytes, int pos);

    int serialize(char *bytes, int pos);

    bool get(nTerm key, long &coordinates);

    bool get(tTerm *key, const int sizeKey, nTerm *value);

    bool get(nTerm key, TermCoordinates *value);

    Node *put(nTerm key, long coordinateTerms);

    Node *put(tTerm *key, int sizeKey, nTerm value);

    Node *putOrGet(tTerm *key, int sizeKey, nTerm &value,
                   bool &insertResult);

    Node *put(nTerm key, TermCoordinates *value);

    long getKey(int pos);

    void getValueAtPos(int pos, nTerm *value);

    void getValueAtPos(int pos, TermCoordinates *value);

    long smallestNumericKey();

    long largestNumericKey();

    tTerm *smallestTextualKey(int *size);

    tTerm *largestTextualKey(int *size);

    Leaf *getRightSibling();

    void free();

    ~Leaf();
};

#endif /* LEAF_H_ */
