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

#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include "../utils/utils.h"

#include <string>
#include <iostream>
#include <math.h>

using namespace std;

class Hashtable {
private:
    const unsigned int size;
    long *table;
    long (*hash)(const char*, const int);
public:
    Hashtable(const unsigned int size, long (*hash)(const char*, const int));

    long add(const char *el, const int l) {
        long hashcode = hash(el, l);
        unsigned int idx = abs(hashcode % size);
        table[idx]++;
        return hashcode;
    }

    long get(const char *el, const int l) {
        unsigned int idx = abs(hash(el, l) % size);
        return table[idx];
    }

    long get(const string &el) {
        unsigned int idx = abs(hash(el.c_str(), el.size()) % size);
        return table[idx];
    }

    long get(int idx) {
        return table[idx];
    }

    void merge(Hashtable *ht) {
        for (int i = 0; i < size; ++i) {
            table[i] += ht->table[i];
        }
    }

    long getThreshold(int highestN) {
        return Utils::quickSelect(table, size, highestN);
    }

    long getTotalCount() {
        long count = 0;
        for (int i = 0; i < size; ++i) {
            count += table[i];
        }
        return count;
    }

    ~Hashtable() {
        delete[] table;
    }
};

#endif /* HASHTABLE_H_ */
