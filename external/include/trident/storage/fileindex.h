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

#ifndef INDEX_H_
#define INDEX_H_

#include <vector>
#include <algorithm>
#include <string.h>
#include <iostream>

#define INITIAL_SIZE 128

class FileIndex {
private:
    int lengthArrays;
    int lengthAdditionalArrays;

    long *keys;
    short *files;
    int *positions;
    int size;
    int additionalSize;
    long *additionalKeys;
    FileIndex **additionalIndices;

    void checkLengthArrays(int size) {
        if (size >= lengthArrays) {
            int newsize = std::max(std::max(size * 2, INITIAL_SIZE), (int) (lengthArrays * 1.5));
            long *newkeys = new long[newsize];
            int *newpositions = new int[newsize];
            short *newfiles = new short[newsize];
            if (size > 0) {
                memcpy(newkeys, keys, sizeof(long) * lengthArrays);
                memcpy(newpositions, positions, sizeof(int) * lengthArrays);
                memcpy(newfiles, files, sizeof(short) * lengthArrays);
                delete[] keys;
                delete[] positions;
                delete[] files;
            }
            keys = newkeys;
            positions = newpositions;
            files = newfiles;
            lengthArrays = newsize;
        }
    }

    void checkLengthAdditionalArrays(int size) {
        if (size >= lengthAdditionalArrays) {
            int newsize = std::max(std::max(size * 2, INITIAL_SIZE), (int) (lengthAdditionalArrays * 1.5));
            long *newkeys = new long[newsize];
            FileIndex **newindices = new FileIndex*[newsize];
            if (size > 0) {
                memcpy(newkeys, additionalKeys,
                       sizeof(long) * lengthAdditionalArrays);
                memcpy(newindices, additionalIndices,
                       sizeof(FileIndex*) * lengthAdditionalArrays);
                delete[] additionalKeys;
                delete[] additionalIndices;
            }
            additionalKeys = newkeys;
            additionalIndices = newindices;
            lengthAdditionalArrays = newsize;
        }
    }

public:
    FileIndex();
    void unserialize(char* buffer, int *offset);
    short file(int idx);
    int pos(int idx);
    long key(int idx);
    int idx(const long key);
    int idx(const int startIdx, const long key);
    FileIndex *additional_idx(long key);
    int sizeIndex();
    ~FileIndex();

    //Used for insert
    bool isEmpty();
    char* serialize(char *buffer, int &offset, int &maxSize);
    void add(long key, short file, int pos);
    void addAdditionalIndex(long key, FileIndex *idx);
};

#endif /* INDEX_H_ */
