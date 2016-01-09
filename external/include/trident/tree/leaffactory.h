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

#ifndef LEAFFACTORY_H_
#define LEAFFACTORY_H_

#include <trident/tree/treecontext.h>

class LeafFactory {
private:
    TreeContext *context;

    std::vector<Leaf> preallocatedElements;
    int preallocatedSize;
    int maxSize;
    Leaf **elements;
    int size;

public:

    LeafFactory(TreeContext *context, int preallocatedSize, int maxSize) :
        preallocatedElements(preallocatedSize, Leaf(context)), maxSize(
            maxSize) {
        this->context = context;
        this->preallocatedSize = preallocatedSize;
        elements = new Leaf*[maxSize];
        size = 0;
    }

    Leaf *get() {
        if (size > 0) {
            return elements[--size];
        } else {
            if (preallocatedSize > 0) {
                Leaf * leaf = &(preallocatedElements[--preallocatedSize]);
                leaf->doNotDeallocate();
                return leaf;
            } else {
                return new Leaf(context);
            }
        }
    }

    void release(Leaf *el) {
        el->free();
        if (size < maxSize) {
            elements[size++] = el;
        } else if (el->shouldDeallocate()) {
            delete el;
        }
    }

    ~LeafFactory() {
        for (int i = 0; i < size; ++i) {
            if (elements[i]->shouldDeallocate())
                delete elements[i];
        }
        delete[] elements;
    }
};

#endif /* LEAFFACTORY_H_ */
