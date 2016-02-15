/*
 * root.h
 *
 *  Created on: Oct 7, 2013
 *      Author: jacopo
 */

#ifndef ROOT_H_
#define ROOT_H_

#include <trident/tree/treeitr.h>
#include <trident/tree/leaffactory.h>
#include <trident/kb/consts.h>
#include <trident/utils/propertymap.h>

#include <string>

class Node;
class TreeContext;
class Value;
class StringBuffer;

typedef enum params {
    MAX_EL_PER_NODE,
    FILE_MAX_SIZE,
    MAX_N_OPENED_FILES,
    CACHE_MAX_SIZE,
    NODE_MIN_BYTES,
    MAX_NODES_IN_CACHE,
    TEXT_KEYS,
    TEXT_VALUES,
    COMPRESSED_NODES,
    LEAF_SIZE_FACTORY,
    LEAF_SIZE_PREALL_FACTORY,
    LEAF_MAX_INTERNAL_LINES,
    LEAF_MAX_PREALL_INTERNAL_LINES,
    LEAF_ARRAYS_FACTORY_SIZE,
    LEAF_ARRAYS_PREALL_FACTORY_SIZE,
    NODE_KEYS_FACTORY_SIZE,
    NODE_KEYS_PREALL_FACTORY_SIZE
} TreeParams;

class Root {
private:
    Cache *cache;
    StringBuffer *stringbuffer;
    Node *rootNode;
    TreeContext *context;
    const bool readOnly;
    const string path;

    PreallocatedArraysFactory<long> *nodesKeysFactory;
    PreallocatedFactory<Coordinates> *ilFactory;
    PreallocatedArraysFactory<Coordinates*> *ilBufferFactory;

    void flushChildrenToCache();

public:
    Root(std::string path, StringBuffer *buffer, bool readOnly,
         PropertyMap &conf);

    bool get(tTerm *key, const int sizeKey, nTerm *value);

    bool get(nTerm key, TermCoordinates *value);

    bool get(nTerm key, long &coordinates);

    void put(nTerm key, TermCoordinates *value);

    bool insertIfNotExists(tTerm *key, int sizeKey, nTerm &value);

    void put(nTerm key, long coordinates);

    TreeItr *itr();

    ~Root();
};

#endif /* ROOT_H_ */
