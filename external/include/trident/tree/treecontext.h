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

#ifndef TREECONTEXT_H_
#define TREECONTEXT_H_

#include <trident/tree/intermediatenode.h>
#include <trident/tree/leaf.h>

#include <tridentcompr/utils/factory.h>

class Cache;
class StringBuffer;

class TreeContext {
private:
    Cache* const cache;
    StringBuffer* const buffer;
    const bool readOnly;
    const int maxNElementsPerNode;
    const int minNElementsPerNode;
    const bool textualKeys;
    const bool textualValues;
    PreallocatedFactory<Coordinates>* const leavesElFactory;
    PreallocatedArraysFactory<Coordinates*>* const leavesBufferFactory;
    PreallocatedArraysFactory<long>* const nodeKeyFactory;
    long nodeCounter;

public:
    TreeContext(Cache *cache, StringBuffer *buffer, bool readOnly,
                int maxElementsPerNode, bool textualKeys, bool textualValues,
                PreallocatedFactory<Coordinates> *leavesElFactory,
                PreallocatedArraysFactory<Coordinates*> *leavesBufferFactory,
                PreallocatedArraysFactory<long> *nodeKeyFactory) :
        cache(cache), buffer(buffer), readOnly(readOnly), maxNElementsPerNode(
            maxElementsPerNode), minNElementsPerNode(
                maxElementsPerNode / 2), textualKeys(textualKeys), textualValues(
                    textualValues), leavesElFactory(leavesElFactory), leavesBufferFactory(
                        leavesBufferFactory), nodeKeyFactory(nodeKeyFactory) {
        nodeCounter = 0;
    }

    long getNewNodeID() {
        return nodeCounter++;
    }

    Cache * const getCache() const {
        return cache;
    }

    StringBuffer * const getStringBuffer() const {
        return buffer;
    }

    const bool textKeys() const {
        return textualKeys;
    }

    const bool isReadOnly() const {
        return readOnly;
    }

    const int getMaxElementsPerNode() const {
        return maxNElementsPerNode;
    }

    const int getMinElementsPerNode() const {
        return minNElementsPerNode;
    }

    const bool textValues() const {
        return textualValues;
    }

    PreallocatedFactory<Coordinates>* const getIlFactory() const {
        return leavesElFactory;
    }

    PreallocatedArraysFactory<Coordinates*>* const getIlBufferFactory() const {
        return leavesBufferFactory;
    }

    PreallocatedArraysFactory<long>* const getNodesKeyFactory() const {
        return nodeKeyFactory;
    }
};

#endif /* TREECONTEXT_H_ */
