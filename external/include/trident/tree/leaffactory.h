/*
 * leaffactory.h
 *
 *  Created on: Nov 28, 2013
 *      Author: jacopo
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
