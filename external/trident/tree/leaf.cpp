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

#include <trident/tree/leaf.h>
#include <trident/tree/treecontext.h>
#include <trident/tree/cache.h>
#include <trident/kb/consts.h>

#include <tridentcompr/utils/utils.h>

#include <boost/chrono.hpp>

#include <string.h>
#include <assert.h>
#include <climits>

namespace timens = boost::chrono;

Leaf::Leaf(TreeContext *c) :
    Node(c) {
    detailPermutations1 = NULL;
    detailPermutations2 = NULL;
    rawNode = NULL;
    nInstantiatedPositions = 0;
    scanThroughArray = !c->isReadOnly();

    numValue = NULL;
    sNumValue = 0;
}

Leaf::Leaf(const Leaf& other) :
    Node(other.getContext()) {
    detailPermutations1 = NULL;
    detailPermutations2 = NULL;
    rawNode = NULL;
    nInstantiatedPositions = 0;
    scanThroughArray = !getContext()->isReadOnly();

    numValue = NULL;
    sNumValue = 0;
}

void Leaf::clearFirstLevel(int size) {
    if (detailPermutations1 != NULL) {
        if (scanThroughArray) {
            for (int i = 0;
                    i < size && i < getContext()->getMinElementsPerNode();
                    ++i) {
                if (detailPermutations1[i] != NULL) {
                    Coordinates *next = detailPermutations1[i]->next;
                    getContext()->getIlFactory()->release(
                        detailPermutations1[i]);
                    while (next != NULL) {
                        Coordinates *el = next;
                        next = el->next;
                        getContext()->getIlFactory()->release(el);
                    }
                    detailPermutations1[i] = NULL;
                }
            }
            size -= getContext()->getMinElementsPerNode();
            for (int i = 0; i < size; ++i) {
                if (detailPermutations2[i] != NULL) {
                    Coordinates *next = detailPermutations2[i]->next;
                    getContext()->getIlFactory()->release(
                        detailPermutations2[i]);
                    while (next != NULL) {
                        Coordinates *el = next;
                        next = el->next;
                        getContext()->getIlFactory()->release(el);
                    }
                    detailPermutations2[i] = NULL;
                }
            }
        } else {
            //Quick way. Just remove the elements at the specified positions
            for (int i = 0; i < nInstantiatedPositions; ++i) {
                int p = instantiatedPositions[i];

                Coordinates **array;
                if (p >= getContext()->getMinElementsPerNode()) {
                    array = detailPermutations2;
                    p -= getContext()->getMinElementsPerNode();
                } else {
                    array = detailPermutations1;
                }

                Coordinates *next = array[p]->next;
                getContext()->getIlFactory()->release(array[p]);

                while (next != NULL) {
                    Coordinates *el = next;
                    next = el->next;
                    getContext()->getIlFactory()->release(el);
                }
                array[p] = NULL;
            }
        }
        nInstantiatedPositions = 0;
        if (getContext()->isReadOnly())
            scanThroughArray = false;

        if (detailPermutations1 != NULL) {
            getContext()->getIlBufferFactory()->release(detailPermutations1);
            detailPermutations1 = NULL;
        }

        if (detailPermutations2 != NULL) {
            getContext()->getIlBufferFactory()->release(detailPermutations2);
            detailPermutations2 = NULL;
        }
    }
}

int Leaf::unserialize_numberlist(char *bytes, int pos) {
    //Check if the size of the array is large enough
    if (numValue == NULL
            || (getContext()->isReadOnly() && sNumValue < getCurrentSize())) {
        if (numValue != NULL) {
            delete[] numValue;
        }

        if (getContext()->isReadOnly()) {
            sNumValue = getCurrentSize();
        } else {
            sNumValue = getContext()->getMaxElementsPerNode();
        }
        numValue = new nTerm[sNumValue];
    }

    //Copy the values
    for (int i = 0; i < getCurrentSize(); i++) {
        numValue[i] = Utils::decode_vlong2(bytes, &pos);
    }
    return pos;
}

int Leaf::unserialize_values(char *bytes, int pos, int previousSize) {
    if (detailPermutations1 == NULL) {
        detailPermutations1 = getContext()->getIlBufferFactory()->get();
    }

    if (getCurrentSize()
            > getContext()->getMinElementsPerNode() && detailPermutations2 == NULL) {
        detailPermutations2 = getContext()->getIlBufferFactory()->get();
    }
    rawNode = bytes + pos;

    if (!getContext()->isReadOnly()) {
        //Unpack completely the node
        for (int i = 0; i < getCurrentSize(); ++i) {
            if (i < getContext()->getMinElementsPerNode()) {
                detailPermutations1[i] = parseInternalLine(i);
            } else {
                detailPermutations2[i - getContext()->getMinElementsPerNode()] =
                    parseInternalLine(i);
            }
        }
    }

    return pos;
}

int Leaf::unserialize(char *bytes, int pos) {

    //Check if the leaf was already existing
    int previousSize = getCurrentSize();
    pos = Node::unserialize(bytes, pos);

    if (getContext()->textKeys() || getContext()->textValues()) {
        pos = unserialize_numberlist(bytes, pos);
    } else {
//      if (!getContext()->textValues()) {
        pos = unserialize_values(bytes, pos, previousSize);
//      } else {
//          pos = unserialize_text(bytes, pos, previousSize);
//      }
    }

    return pos;
}

int Leaf::serialize_numberlist(char *bytes, int pos) {
    for (int i = 0; i < getCurrentSize(); i++) {
        pos = Utils::encode_vlong2(bytes, pos, numValue[i]);
    }
    return pos;
}

//int Leaf::serialize_text(char *bytes, int pos) {
//  /*** STRINGS ***/
//  // We reserve 4 * getCurrentBytes to store the positions of the
//  // strings.
//  int currentSize = getCurrentSize();
//  int origPos = pos;
//  pos += 4 * currentSize;
//  int startPos = pos;
//
//  //Copy the first string
//  memcpy(bytes + pos, strings, endStrings[0]);
//  pos += endStrings[0];
//  Utils::encode_int(bytes, origPos, endStrings[0]);
//  origPos += 4;
//
//  for (int i = 1; i < currentSize; ++i) {
//      // 1- Calculate the number of equal characters
//      int eq_bytes = Utils::commonPrefix((tTerm*) strings, 0, endStrings[0],
//              (tTerm *) strings, endStrings[i - 1], endStrings[i]);
//
//      // 2- Write the number of equal characters
//      pos = Utils::encode_vint2(bytes, pos, eq_bytes);
//
//      // 3- Write the length of the remaining strings
//      int remSize = endStrings[i] - endStrings[i - 1] - eq_bytes;
//      pos = Utils::encode_vint2(bytes, pos, remSize);
//
//      // 4- Write the remaining string
//      memcpy(bytes + pos, strings + endStrings[i] - remSize, remSize);
//      pos += remSize;
//
//      Utils::encode_int(bytes, origPos, pos - startPos);
//      origPos += 4;
//  }
//  return pos;
//}

int Leaf::serialize_values(char *bytes, int pos) {
    // The original buffer (b) is used to store the combinations per
    // entry. The following buffer starts later and stores the
    // positions.
    int posCombinations = pos;
    int posValueStarts = posCombinations + getCurrentSize();
    int posValues = posValueStarts + getCurrentSize() * 2;
    int posValueOrig = posValues;
    posValues += 4;

    for (int i = 0; i < getCurrentSize(); i++) {
        // Write the starting pos on bufferPositions.
        int diff = posValues - posValueOrig;
        if (diff > USHRT_MAX) {
            BOOST_LOG_TRIVIAL(error) << "The value of the nodes are too high to be addressed by 2 bytes. Needs to find a workaround for that...";
            exit(1);
        }
        Utils::encode_short(bytes, posValueStarts, diff);
        posValueStarts += 2;

        // Write the elements on elementsBuffer
        Coordinates *c = NULL;
        if (i < getContext()->getMinElementsPerNode()) {
            c = detailPermutations1[i];
        } else {
            c = detailPermutations2[i - getContext()->getMinElementsPerNode()];
        }

        int combinations = 0;
        while (c != NULL) {
            posValues = Utils::encode_vlong2(bytes, posValues, c->nElements);
            posValues = Utils::encode_vint2(bytes, posValues, c->file);
            posValues = Utils::encode_vint2(bytes, posValues, c->posInFile);
            bytes[posValues++] = c->strategy;
            combinations |= 1 << c->permutation;
            c = c->next;
        }

        // Write the combinations of the permutations
        bytes[posCombinations++] = (char) combinations;
    }

    // Write the total size of the elementsBuffer at the beginning
    int sizeElementsBuffer = posValues - posValueOrig;
    Utils::encode_int(bytes, posValueOrig, sizeElementsBuffer);
    return posValues;
}

int Leaf::serialize(char *bytes, int pos) {
    pos = Node::serialize(bytes, pos);

    if (getContext()->textKeys() || getContext()->textValues()) {
        pos = serialize_numberlist(bytes, pos);
    } else {
//      if (!getContext()->textValues()) {
        pos = serialize_values(bytes, pos);
//      } else {
//          pos = serialize_text(bytes, pos);
//      }
    }

    return pos;
}

//bool Leaf::get(nTerm key, tTerm *value) {
//  int p = pos(key);
//  if (p < 0) {
//      return false;
//  }
//
//  if (p == 0) {
//      int len = Utils::decode_int(strings, p);
//      memcpy(value, pointerFirstString, len);
//      value[len] = 0;
//  } else {
//      int startPos = Utils::decode_int(strings, (p - 1) * 4);
//      int eq_bytes = Utils::decode_vint2(pointerFirstString, &startPos);
//      int len = Utils::decode_vint2(pointerFirstString, &startPos);
//      memcpy(value, pointerFirstString, eq_bytes);
//      memcpy(value + eq_bytes, pointerFirstString + startPos, len);
//      value[eq_bytes + len] = 0;
//  }
//
//  return true;
//}
bool Leaf::get(nTerm key, long &value) {
    int p = pos(key);
    if (p < 0) {
        return false;
    } else {
        value = numValue[p];
        return true;
    }
}

bool Leaf::get(nTerm key, TermCoordinates *value) {
    int p = pos(key);
    if (p < 0) {
        return false;
    }
    getValueAtPos(p, value);
    return true;
}

bool Leaf::get(tTerm *key, const int sizeKey, nTerm *value) {
    int p = pos(key, sizeKey);
    if (p < 0) {
        return false;
    } else {
        getValueAtPos(p, value);
        return true;
    }
}

void insertStringIntoBuffer(tTerm *value, int sizeValue, int p, char* strings,
                            int stringsSize, int *endStrings, int nValues) {
    // Insert new strings
    if (p == nValues - 1) {
        // Last element
        endStrings[p] = stringsSize + sizeValue;
        memcpy(strings + stringsSize, value, sizeValue);
    } else {
        // Shifting is required
        int startPos = p > 0 ? endStrings[p - 1] : 0;
        memmove(strings + startPos + sizeValue, strings + startPos,
                stringsSize - startPos);
        memcpy(strings + startPos, value, sizeValue);
        for (int i = nValues - 1; i >= p + 1; i--) {
            endStrings[i] = endStrings[i - 1] + sizeValue;
        }
        endStrings[p] = startPos + sizeValue;
    }
}

//void Leaf::enlargeStringsBuffer(Leaf *leaf, int occupiedSize, int addedBytes) {
//  if (occupiedSize + addedBytes >= leaf->sizeStrings) {
//      int newSize = max(leaf->sizeStrings + addedBytes,
//              max(leaf->sizeStrings * 2,
//                      30 * leaf->getContext()->getMaxElementsPerNode()));
//      char *newStrings = new char[newSize];
//      if (leaf->sizeStrings > 0) {
//          memcpy(newStrings, leaf->strings, leaf->sizeStrings);
//      }
//      leaf->sizeStrings = newSize;
//      if (leaf->strings != NULL) {
//          delete[] leaf->strings;
//      }
//      leaf->strings = newStrings;
//  }
//}

//Node *Leaf::put(nTerm key, tTerm *value, int sizeValue) {
//  setState(STATE_MODIFIED);
//  int p = pos(key);
//  if (p < 0) {
//      p = -p - 1;
//      if (shouldSplit()) {
//          Leaf *l = getContext()->getCache()->newLeaf();
//          l->setId(getContext()->getNewNodeID());
//          l->setParent(getParent());
//          //Copy strings
//          int minSize = getContext()->getMinElementsPerNode();
//          int sizeStringsToCopy =
//                  endStrings[getContext()->getMaxElementsPerNode() - 1]
//                          - endStrings[minSize - 1];
//          enlargeStringsBuffer(l, 0, sizeStringsToCopy);
//
//          memcpy(l->strings, strings + endStrings[minSize - 1],
//                  sizeStringsToCopy);
//
//          //Copy endStrings
//          if (l->endStrings == NULL) {
//              l->endStrings = new int[getContext()->getMaxElementsPerNode()];
//          }
//          for (int i = 0; i < minSize; ++i) {
//              l->endStrings[i] = endStrings[minSize + i]
//                      - endStrings[minSize - 1];
//          }
//
//          split(l);
//          if (p < getContext()->getMinElementsPerNode()) {
//              putkeyAt(key, p);
//              int bufferSize =
//                      getCurrentSize() > 1 ?
//                              endStrings[getCurrentSize() - 2] : 0;
//              enlargeStringsBuffer(this, bufferSize, sizeValue);
//              insertStringIntoBuffer(value, sizeValue, p, strings, bufferSize,
//                      endStrings, getCurrentSize());
//          } else {
//              p -= getContext()->getMinElementsPerNode();
//              l->putkeyAt(key, p);
//              int bufferSize =
//                      l->getCurrentSize() > 1 ?
//                              l->endStrings[l->getCurrentSize() - 2] : 0;
//              enlargeStringsBuffer(l, bufferSize, sizeValue);
//              insertStringIntoBuffer(value, sizeValue, p, l->strings,
//                      bufferSize, l->endStrings, l->getCurrentSize());
//          }
//
//          l->setState(STATE_MODIFIED);
//          return l;
//      } else {
//          /*** New insert ***/
//          if (endStrings == NULL) {
//              endStrings = new int[getContext()->getMaxElementsPerNode()];
//          }
//
//          //Make sure that the container of the strings is big enough
//          int currentBufferSize =
//                  getCurrentSize() > 0 ? endStrings[getCurrentSize() - 1] : 0;
//          enlargeStringsBuffer(this, currentBufferSize, sizeValue);
//          putkeyAt(key, p);
//          insertStringIntoBuffer(value, sizeValue, p, strings,
//                  currentBufferSize, endStrings, getCurrentSize());
//      }
//  } else {
//      /*** Replace current value ***/
//      int oldSize = endStrings[p];
//      int startPos = p > 0 ? endStrings[p - 1] : 0;
//
//      // Increase or decrease the size
//      int sizeOldString = oldSize - startPos;
//      if (sizeOldString != sizeValue && p < getCurrentSize()) {
//          if (sizeValue > sizeOldString
//                  && (endStrings[getCurrentSize() - 1] + sizeValue
//                          - sizeOldString >= sizeStrings)) {
//              sizeStrings = sizeStrings + sizeValue - sizeOldString;
//              char *newStrings = new char[sizeStrings];
//              memcpy(newStrings, strings + startPos, startPos);
//              memcpy(newStrings + startPos + sizeValue, strings + oldSize,
//                      endStrings[getCurrentSize() - 1] - oldSize);
//              delete[] strings;
//              strings = newStrings;
//          } else {
//              // Shift the elements inside the currentBuffer
//              memmove(strings + startPos + sizeValue, strings + oldSize,
//                      (endStrings[getCurrentSize() - 1] - oldSize)
//                              * sizeof(strings));
//          }
//
//          endStrings[p] = startPos + sizeValue;
//          for (int i = p + 1; i < getCurrentSize(); ++i) {
//              endStrings[i] += sizeValue - sizeOldString;
//          }
//      }
//      memcpy(strings + startPos, value, sizeof(tTerm) * sizeValue);
//  }
//  return NULL;
//}
Node *Leaf::put(nTerm key, long coordinates) {
    setState(STATE_MODIFIED);
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
        if (shouldSplit()) {
            Leaf *l = getContext()->getCache()->newLeaf();
            l->setId(getContext()->getNewNodeID());
            l->setParent(getParent());
            if (l->sNumValue < getContext()->getMaxElementsPerNode()) {
                if (l->numValue != NULL) {
                    delete[] l->numValue;
                }
                l->numValue = new nTerm[getContext()->getMaxElementsPerNode()];
                l->sNumValue = getContext()->getMaxElementsPerNode();
            }
            memcpy(l->numValue,
                   numValue + getContext()->getMinElementsPerNode(),
                   sizeof(nTerm) * getContext()->getMinElementsPerNode());
            split(l);

            if (p < getContext()->getMinElementsPerNode()) {
                put(key, coordinates);
            } else {
                l->put(key, coordinates);
            }

            l->setState(STATE_MODIFIED);
            return l;
        } else {
            putkeyAt(key, p);

            if (sNumValue < getContext()->getMaxElementsPerNode()) {
                if (numValue != NULL) {
                    delete[] numValue;
                }
                numValue = new nTerm[getContext()->getMaxElementsPerNode()];
                sNumValue = getContext()->getMaxElementsPerNode();
            }

            //Make space for the new value
            if (p != getCurrentSize() - 1) {
                memmove(numValue + p + 1, numValue + p,
                        (getCurrentSize() - 1 - p) * sizeof(nTerm));
            }

            numValue[p] = coordinates;
        }
    } else {
        //Simply update the current value
        numValue[p] = coordinates;
    }
    return NULL;
}

Node *Leaf::insertAtPosition(int p, tTerm *key, int sizeKey, nTerm value) {
    if (shouldSplit()) {
        Leaf *l = getContext()->getCache()->newLeaf();
        l->setId(getContext()->getNewNodeID());
        l->setParent(getParent());

        if (l->sNumValue < getContext()->getMaxElementsPerNode()) {
            if (l->numValue != NULL) {
                delete[] l->numValue;
            }
            l->numValue = new nTerm[getContext()->getMaxElementsPerNode()];
            l->sNumValue = getContext()->getMaxElementsPerNode();
        }
        memcpy(l->numValue, numValue + getContext()->getMinElementsPerNode(),
               sizeof(nTerm) * getContext()->getMinElementsPerNode());
        split(l);

        if (p < getContext()->getMinElementsPerNode()) {
            putkeyAt(key, sizeKey, p);
            if (p != getCurrentSize() - 1) {
                memmove(numValue + p + 1, numValue + p,
                        (getCurrentSize() - 1 - p) * sizeof(nTerm));
            }
            numValue[p] = value;
        } else {
            p -= getContext()->getMinElementsPerNode();
            l->putkeyAt(key, sizeKey, p);
            if (p != l->getCurrentSize() - 1) {
                memmove(l->numValue + p + 1, l->numValue + p,
                        (l->getCurrentSize() - 1 - p) * sizeof(nTerm));
            }
            l->numValue[p] = value;
        }

        l->setState(STATE_MODIFIED);
        return l;
    } else {
        putkeyAt(key, sizeKey, p);

        if (sNumValue < getContext()->getMaxElementsPerNode()) {
            if (numValue != NULL) {
                delete[] numValue;
            }
            numValue = new nTerm[getContext()->getMaxElementsPerNode()];
            sNumValue = getContext()->getMaxElementsPerNode();
        }

        //Make space for the new value
        if (p != getCurrentSize() - 1) {
            memmove(numValue + p + 1, numValue + p,
                    (getCurrentSize() - 1 - p) * sizeof(nTerm));
        }
        numValue[p] = value;
    }
    return NULL;
}

Node *Leaf::put(tTerm *key, int sizeKey, nTerm value) {
    setState(STATE_MODIFIED);
    int p = pos(key, sizeKey);
    if (p < 0) {
        p = -p - 1;
        return insertAtPosition(p, key, sizeKey, value);
    } else {
        //Simply update the current value
        numValue[p] = value;
        return NULL;
    }
}

Node *Leaf::putOrGet(tTerm *key, int sizeKey, nTerm &value,
                     bool &insertResult) {
    int p = pos(key, sizeKey);
    if (p < 0) {
        p = -p - 1;
        insertResult = true;
        setState(STATE_MODIFIED);
        return insertAtPosition(p, key, sizeKey, value);
    } else {
        insertResult = false;
        value = numValue[p];
        return NULL;
    }
}

Node *Leaf::put(nTerm key, TermCoordinates *value) {
    setState(STATE_MODIFIED);
    int p = pos(key);
    if (p < 0) {
        p = -p - 1;
        if (shouldSplit()) {
            Leaf *l = getContext()->getCache()->newLeaf();
            l->setId(getContext()->getNewNodeID());
            l->setParent(getParent());
            l->detailPermutations1 = detailPermutations2;
            l->detailPermutations2 = NULL;
            detailPermutations2 = NULL;
            split(l);

            if (p < getContext()->getMinElementsPerNode()) {
                put(key, value);
            } else {
                l->put(key, value);
            }

            l->setState(STATE_MODIFIED);
            return l;
        } else {
            if (p < getContext()->getMinElementsPerNode()) {
                if (getCurrentSize() >= getContext()->getMinElementsPerNode()) {
                    //Shift the second array
                    int sizeSecondArray = getCurrentSize()
                                          - getContext()->getMinElementsPerNode();
                    if (sizeSecondArray > 0) {
                        memmove(detailPermutations2 + 1, detailPermutations2,
                                sizeof(Coordinates*) * sizeSecondArray);
                    } else if (detailPermutations2 == NULL) {
                        detailPermutations2 =
                            getContext()->getIlBufferFactory()->get();
                    }
                    detailPermutations2[0] =
                        detailPermutations1[getContext()->getMinElementsPerNode()
                                            - 1];
                    if (p < getContext()->getMinElementsPerNode() - 1) {
                        memmove(detailPermutations1 + p + 1,
                                detailPermutations1 + p,
                                sizeof(Coordinates*)
                                * (getContext()->getMinElementsPerNode()
                                   - p - 1));
                    }
                } else if (p < getCurrentSize()) {
                    memmove(detailPermutations1 + p + 1,
                            detailPermutations1 + p,
                            sizeof(Coordinates*) * (getCurrentSize() - p));
                } else if (detailPermutations1 == NULL) {
                    detailPermutations1 =
                        getContext()->getIlBufferFactory()->get();
                }
                detailPermutations1[p] = addCoordinates(NULL, value);
                putkeyAt(key, p);
            } else {
                p -= getContext()->getMinElementsPerNode();
                if (p
                        < getCurrentSize()
                        - getContext()->getMinElementsPerNode()) {
                    memmove(detailPermutations2 + p + 1,
                            detailPermutations2 + p,
                            sizeof(Coordinates*)
                            * (getCurrentSize()
                               - getContext()->getMinElementsPerNode()
                               - p));
                }
                if (detailPermutations2 == NULL) {
                    detailPermutations2 =
                        getContext()->getIlBufferFactory()->get();
                }
                detailPermutations2[p] = addCoordinates(NULL, value);
                putkeyAt(key, p + getContext()->getMinElementsPerNode());
            }
        }
    } else {
        if (p < getContext()->getMinElementsPerNode()) {
            detailPermutations1[p] = addCoordinates(detailPermutations1[p],
                                                    value);
        } else {
            p -= getContext()->getMinElementsPerNode();
            detailPermutations2[p] = addCoordinates(detailPermutations2[p],
                                                    value);
        }
    }
    return NULL;
}

Coordinates * Leaf::addCoordinates(Coordinates * list,
                                   TermCoordinates * value) {
    Coordinates *first = list;
    Coordinates *prev = NULL;
    Coordinates *current = list;

    for (uint8_t i = 0; i < N_PARTITIONS; ++i) {
        if (value->exists(i)) {
            while (current != NULL && current->permutation < i) {
                prev = current;
                current = current->next;
            }
            if (current == NULL) {
                current = getContext()->getIlFactory()->get();
                if (prev != NULL) {
                    prev->next = current;
                } else {
                    first = current;
                }
                current->next = NULL;
            } else if (current->permutation > i) {
                Coordinates *newel = getContext()->getIlFactory()->get();
                newel->next = current;
                if (prev != NULL) {
                    prev->next = newel;
                } else {
                    first = newel;
                }
                current = newel;
            }

            current->permutation = i;
            current->file = value->getFileIdx(i);
            current->posInFile = value->getMark(i);
            current->nElements = value->getNElements(i);
            current->strategy = value->getStrategy(i);
        } else if (current != NULL && current->permutation == i) {
            if (prev == NULL) {
                first = current->next;
            } else {
                prev->next = current->next;
            }
            Coordinates *tmp = current;
            current = current->next;
            getContext()->getIlFactory()->release(tmp);
        }
    }
    return first;
}

/*** All possible combinations of permutations ***/
//n. combinations
const char NCOMBS[64] = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6};
//idx elements in combinations
const char ICOMBS[64] = {0, 0, 1, 0, 2, 3, 1, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 0, 23};
const uint8_t COMBS[29] = {0, 1, 2, 0, 2, 0, 3, 1, 4, 2, 5, 0, 1, 3, 4, 1, 2, 4, 5, 0, 2, 3, 5, 0, 1, 2, 3, 4, 5};

Coordinates *Leaf::parseInternalLine(const int pos) {
    unsigned char permutations = rawNode[pos];
    int startPos = (unsigned short) Utils::decode_short((const char*)rawNode,
                   getCurrentSize() + pos * 2);
    startPos += getCurrentSize() * 3;

    Coordinates *first = NULL;
    Coordinates *previousEl = NULL;

    int idx = ICOMBS[permutations];
    for (int i = 0; i < NCOMBS[permutations]; ++i) {
        Coordinates *currentEl = getContext()->getIlFactory()->get();
        currentEl->nElements = Utils::decode_vlong2(rawNode, &startPos);
        currentEl->file = (uint16_t) Utils::decode_vint2(rawNode, &startPos);
        currentEl->posInFile = Utils::decode_vint2(rawNode, &startPos);
        currentEl->strategy = rawNode[startPos++];
        currentEl->permutation = COMBS[idx + i];
        currentEl->next = NULL;

        if (previousEl != NULL) {
            previousEl->next = currentEl;
        }

        if (first == NULL) {
            first = currentEl;
        }

        previousEl = currentEl;
    }

    if (!scanThroughArray) {
        if (nInstantiatedPositions < MAX_INSTANTIED_POS) {
            instantiatedPositions[nInstantiatedPositions++] = (short) pos;
        } else {
            scanThroughArray = true;
        }
    }

    return first;
}

long Leaf::getKey(int pos) {
    return keyAt(pos);
}

long Leaf::smallestNumericKey() {
    return localSmallestNumericKey();
}

long Leaf::largestNumericKey() {
    return localLargestNumericKey();
}

tTerm *Leaf::smallestTextualKey(int *size) {
    return localSmallestTextualKey(size);
}

tTerm *Leaf::largestTextualKey(int *size) {
    return localLargestTextualKey(size);
}

void Leaf::getValueAtPos(int pos, TermCoordinates *value) {
    Coordinates *el = NULL;

    if (pos >= getContext()->getMinElementsPerNode()) {
        if (detailPermutations2[pos - getContext()->getMinElementsPerNode()]
                == NULL) {
            detailPermutations2[pos - getContext()->getMinElementsPerNode()] =
                parseInternalLine(pos);
        }
        el = detailPermutations2[pos - getContext()->getMinElementsPerNode()];
    } else {
        if (detailPermutations1[pos] == NULL) {
            detailPermutations1[pos] = parseInternalLine(pos);
        }
        el = detailPermutations1[pos];
    }
    value->set(el);
}

void Leaf::getValueAtPos(int pos, nTerm *value) {
    *value = numValue[pos];
}

Leaf * Leaf::getRightSibling() {
    IntermediateNode *parent = getParent();
    if (parent == NULL) {
        return this;
    }

    int pos = parent->getPosChild(this);
    while (pos == parent->getCurrentSize() && parent->getParent() != NULL) {
        pos = parent->getParent()->getPosChild(parent);
        parent = parent->getParent();
    }

//Move to the next child
    if (pos < parent->getCurrentSize()) {
        pos++;
        Node *child = parent->getChildAtPos(pos);
        while (child->canHaveChildren()) {
            child = ((IntermediateNode *) child)->getChildAtPos(0);
        }
        return (Leaf *) child;
    } else {
        return this;
    }
}

void Leaf::free() {
    clearFirstLevel(getCurrentSize());
}

Leaf::~Leaf() {
    if (detailPermutations1 != NULL) {
        getContext()->getIlBufferFactory()->release(detailPermutations1);
    }

    if (detailPermutations2 != NULL) {
        getContext()->getIlBufferFactory()->release(detailPermutations2);
    }
    if (numValue != NULL) {
        delete[] numValue;
    }
}

