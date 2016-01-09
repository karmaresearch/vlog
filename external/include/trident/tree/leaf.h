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
