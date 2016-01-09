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

#ifndef NODE_H_
#define NODE_H_

#include <trident/tree/coordinates.h>
#include <trident/kb/consts.h>

#include <tridentcompr/main/consts.h>

#include <vector>
#include <iostream>

#define STATE_MODIFIED 0
#define STATE_UNMODIFIED 1

using namespace std;

class IntermediateNode;
class TreeContext;

class Node {
private:
    long id;
    IntermediateNode *parent;
    TreeContext * const context;
    int currentSize;
    bool deallocate;

    long firstKey;
    long *keys;
    bool consecutive;
    int consecutiveStep;
    char state;

protected:
    int pos(long key);

    int pos(tTerm *key, int size);

    long localSmallestNumericKey() {
        return firstKey;
    }

    long localLargestNumericKey();

    tTerm *localSmallestTextualKey(int *size);

    tTerm *localLargestTextualKey(int *size);

    long keyAt(int pos);

    bool shouldSplit();

    void split(Node *node);

    void removeKeyAtPos(int pos);

public:

    Node(TreeContext *c) :
        id(0), context(c), currentSize(0) {
        this->parent = NULL;
        deallocate = true;
        keys = NULL;
        firstKey = 0;
        consecutive = true;
        consecutiveStep = 1;
//      endStrings = NULL;
//      strings = NULL;
//      wStrings = NULL;
//      sStrings = 0;
        state = STATE_UNMODIFIED;
    }

    void setId(long id) {
        this->id = id;
    }

    void doNotDeallocate() {
        deallocate = false;
    }

    bool shouldDeallocate() {
        return deallocate;
    }

    void setParent(IntermediateNode *p) {
        parent = p;
    }

    IntermediateNode *getParent() {
        return parent;
    }

    int getCurrentSize() {
        return currentSize;
    }

    void setCurrentSize(int size) {
        this->currentSize = size;
    }

    long getId() {
        return id;
    }

    TreeContext *getContext() const {
        return context;
    }

    char getState() {
        return state;
    }

    void setState(const char state) {
        this->state = state;
    }

    void removeLastKey() {
        removeKeyAtPos(currentSize - 1);
    }

    void removeFirstKey() {
        removeKeyAtPos(0);
    }

    virtual Node *getChild(const int p) {
        return NULL;
    }

    void putkeyAt(nTerm key, int pos);

    void putkeyAt(tTerm *key, int sizeKey, int pos);

    /*** VIRTUAL METHODS ***/

    virtual bool canHaveChildren() {
        return false;
    }

    virtual Node *getChildForKey(long key) {
        return NULL;
    }

    virtual int unserialize(char* bytes, int pos) = 0;

    virtual int serialize(char* bytes, int pos);

    virtual long smallestNumericKey() = 0;

    virtual long largestNumericKey() = 0;

    virtual tTerm *smallestTextualKey(int *size) = 0;

    virtual tTerm *largestTextualKey(int *size) = 0;

    virtual Node *getChildForKey(tTerm *key, int sizeKey) {
        return NULL;
    }

    virtual bool get(nTerm key, long &coordinates) = 0;

    virtual bool get(tTerm *key, int sizeKey, nTerm *value) = 0;

    virtual bool get(nTerm key, TermCoordinates *value) = 0;

    virtual Node *put(nTerm key, long coordinatesTTerm) = 0;

    virtual Node *put(tTerm *key, int sizeKey, nTerm value) = 0;

    virtual Node *putOrGet(tTerm *key, int sizeKey, nTerm &value, bool &insertResult) = 0;

    virtual Node *put(nTerm key, TermCoordinates *value) = 0;

    virtual ~Node();
};

#endif /* NODE_H_ */
