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
