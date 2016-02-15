/*
 * node.cpp
 *
 *  Created on: Oct 6, 2013
 *      Author: jacopo
 */

#include <trident/tree/node.h>
#include <trident/tree/treecontext.h>
#include <trident/tree/stringbuffer.h>

#include <tridentcompr/utils/utils.h>

#include <algorithm>
#include <string.h>
#include <assert.h>

int Node::pos(long key) {
    if (getCurrentSize() == 0 || key < firstKey) {
        return -1;
    }

    long diff = key - firstKey;
    if (consecutive) {
        if (diff < (getCurrentSize() * consecutiveStep)) {
            if (consecutiveStep > 1) {
                if (diff % consecutiveStep == 0) {
                    return diff / consecutiveStep;
                } else {
                    return -(diff / consecutiveStep + 1) - 1;
                }
            } else {
                return (int) diff;
            }
        } else {
            return -getCurrentSize() - 1;
        }
    }

    if (diff == 0) {
        return 0;
    } else {
        if (getCurrentSize() == 1) {
            return -2;
        } else if (diff == keys[getCurrentSize() - 2]) {
            return getCurrentSize() - 1;
        } else if (diff > keys[getCurrentSize() - 2]) {
            return -getCurrentSize() - 1;
        } else {
            int low = 0;
            int high = getCurrentSize() - 3;
            while (low <= high) {
                int mid = (low + high) >> 1;
                long midVal = keys[mid];
                if (midVal < diff)
                    low = mid + 1;
                else if (midVal > diff)
                    high = mid - 1;
                else
                    return mid + 1; // key found
            }
            return -(low + 2); // key not found.
        }
    }
}

long Node::localLargestNumericKey() {
    if (getCurrentSize() == 1) {
        return firstKey;
    } else {
        if (consecutive) {
            return firstKey + (getCurrentSize() - 1) * consecutiveStep;
        } else {
            return firstKey + keys[getCurrentSize() - 2];
        }
    }
}

int Node::unserialize(char* bytes, int pos) {
    setState(STATE_UNMODIFIED);
    int size = Utils::decode_int(bytes, pos);
    setCurrentSize(size);
    pos += 4;

    if (size > 0) {
        firstKey = Utils::decode_long(bytes, pos);
        pos += 8;
        if (size > 1) {
            char step = bytes[pos++];
            if (step > 0) {
                consecutive = true;
                consecutiveStep = step;
            } else {
                consecutive = false;
                if (keys == NULL) {
                    keys = context->getNodesKeyFactory()->get();
                }

                int *pPos = &pos;
                if (getContext()->textKeys()) {
                    for (int i = 0; i < getCurrentSize() - 1; ++i) {
                        keys[i] = Utils::decode_vlong(bytes, pPos) - firstKey;
                    }
                } else {
                    for (int i = 0; i < getCurrentSize() - 1; ++i) {
                        keys[i] = Utils::decode_vlong(bytes, pPos);
                    }
                }
                pos = *pPos;
            }
        }
    }
    return pos;
}

int Node::serialize(char* bytes, int pos) {
    Utils::encode_int(bytes, pos, getCurrentSize());
    pos += 4;
    if (getCurrentSize() > 0) {
        Utils::encode_long(bytes, pos, firstKey);
        pos += 8;
        if (getCurrentSize() > 1) {
            // Check if we can make it consecutive
            uint8_t step = 0;
            if (consecutive
                    || (keys[0] == 1
                        && keys[getCurrentSize() - 2]
                        == getCurrentSize() - 1)) {
                step = 1;
            } else {
                if (keys[0] > 0 && keys[0] < 256 && keys[getCurrentSize() - 2] % keys[0] == 0
                        && keys[getCurrentSize() - 2] / keys[0]
                        == getCurrentSize() - 1) {
                    // The last value is a multiple of the first value.
                    // Maybe it's a sequence...
                    step = (uint8_t) keys[0];
                    for (int i = 1; i < getCurrentSize() - 1; ++i) {
                        if (keys[i] - keys[i - 1] != step) {
                            step = 0;
                            break;
                        }
                    }
                }
            }

            bytes[pos++] = step;
            if (step == 0) {
                if (getContext()->textKeys()) {
                    for (int i = 0; i < getCurrentSize() - 1; ++i) {
                        //Accepts only position numbers
                        pos = Utils::encode_vlong(bytes, pos,
                                                  keys[i] + firstKey);
                    }
                } else {
                    for (int i = 0; i < getCurrentSize() - 1; ++i) {
                        pos = Utils::encode_vlong(bytes, pos, keys[i]);
                    }
                }
            }
        }
    }
    return pos;
}

long Node::keyAt(int pos) {
    if (consecutive) {
        return firstKey + pos * consecutiveStep;
    } else {
        if (pos > 0) {
            return firstKey + keys[pos - 1];
        } else {
            return firstKey;
        }
    }
}

tTerm *Node::localSmallestTextualKey(int *size) {
    long coordinates = keyAt(0);
    return (tTerm*) getContext()->getStringBuffer()->get(coordinates, *size);
}

tTerm *Node::localLargestTextualKey(int *size) {
    long coordinates = keyAt(getCurrentSize() - 1);
    return (tTerm*) getContext()->getStringBuffer()->get(coordinates, *size);
}

int Node::pos(tTerm *key, int size) {
    int low = 0;
    int high = getCurrentSize() - 1;
    while (low <= high) {
        int mid = (low + high) >> 1;
        long coordinates = keyAt(mid);
        int retVal = getContext()->getStringBuffer()->cmp(coordinates,
                     (char*) key, size);
        if (retVal < 0)
            low = mid + 1;
        else if (retVal > 0)
            high = mid - 1;
        else
            return mid; // key found
    }
    return -(low + 1); // key not found.
}

bool Node::shouldSplit() {
    return getCurrentSize() == context->getMaxElementsPerNode();
}

void Node::split(Node *node) {
    currentSize = node->currentSize = context->getMinElementsPerNode();

    if (consecutive) {
        node->firstKey = firstKey + currentSize;
        node->consecutive = true;
    } else {
        node->firstKey = firstKey + keys[currentSize - 1];
        if (keys[context->getMaxElementsPerNode() - 2] - keys[currentSize - 1]
                + 1 == currentSize) {
            node->consecutive = true;
        } else {
            node->consecutive = false;
            if (node->keys == NULL) {
                node->keys = context->getNodesKeyFactory()->get();
            }
            long diff = node->firstKey - firstKey;
            for (int i = 0; i < currentSize - 1; ++i) {
                node->keys[i] = keys[currentSize + i] - diff;
            }
        }
    }
}

void Node::putkeyAt(nTerm key, int pos) {
    if (pos == 0) {
        // Need to change the first key
        if (getCurrentSize() == 0) {
            firstKey = key;
        } else {
            if (consecutive) {
                long diff = firstKey - key;
//              keys = new long[context->getMaxElementsPerNode() - 1];
                keys = context->getNodesKeyFactory()->get();
                keys[0] = diff;
                for (int i = 1; i < getCurrentSize(); ++i) {
                    keys[i] = diff + i;
                }
                firstKey = key;
                consecutive = false;
            } else {
                memmove(keys + 1, keys, sizeof(long) * (currentSize - 1));
                long diff = firstKey - key;
                keys[0] = diff;
                for (int i = 1; i < getCurrentSize(); ++i) {
                    keys[i] += diff;
                }
                firstKey = key;
            }
        }
    } else {
        if (consecutive) {
            long diff = key - firstKey;
            if (pos != diff) {
                consecutive = false;
//              keys = new long[context->getMaxElementsPerNode() - 1];
                keys = context->getNodesKeyFactory()->get();
                for (int i = 0; i < getCurrentSize() - 1; ++i) {
                    keys[i] = i + 1;
                }
                keys[pos - 1] = diff;
            }
        } else {
            if (pos < getCurrentSize()) {
                memmove(keys + pos, keys + pos - 1,
                        sizeof(long) * (currentSize - pos));
            }
            keys[pos - 1] = key - firstKey;
        }
    }
    currentSize++;
}

void Node::putkeyAt(tTerm *key, int sizeKey, int pos) {
//Warning. This method always inserts a new string. Updates are not supported.
    long startPos = context->getStringBuffer()->getSize();
    context->getStringBuffer()->append((char*) key, sizeKey);
    putkeyAt(startPos, pos);
}

void Node::removeKeyAtPos(int pos) {
    if (getCurrentSize() > 1 && pos < getCurrentSize() - 1) {
        if (consecutive) {
            if (pos == 0) {
                firstKey += 1;
            } else {
                if (keys == NULL) {
//                  keys = new long[context->getMaxElementsPerNode() - 1];
                    keys = context->getNodesKeyFactory()->get();
                }
                // up to pos -1 fill keys with 1s
                for (int i = 0; i < pos - 1; ++i) {
                    keys[i] = i + 1;
                }
                for (int i = pos - 1; i < getCurrentSize() - 2; ++i) {
                    keys[i] = i + 2;
                }
                consecutive = false;
            }
        } else {
            if (pos == 0) {
                long diff = keys[0];
                firstKey += diff;
                for (int i = 0; i < getCurrentSize() - 2; ++i) {
                    keys[i] = keys[i + 1] - diff;
                }
            } else {
                // Shift the other elements by one position
                memmove(keys + pos - 1, keys + pos,
                        (getCurrentSize() - 1 - pos) * sizeof(long));
            }
        }
    }
    currentSize--;
}

Node::~Node() {
    if (keys != NULL) {
//          delete[] keys;
        context->getNodesKeyFactory()->release(keys);
        keys = NULL;
    }
}
