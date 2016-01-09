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

#ifndef HASHFUNCTIONS_H_
#define HASHFUNCTIONS_H_

#include "murmurhash3.h"

#include <boost/functional/hash.hpp>

#include <cstring>

using namespace std;

class Hashes {
public:
    static long murmur3_56(const char* s, const int size) {
        char output[16];
        MurmurHash3_x64_128(s, size, 0, output);
        long number = output[0];
        number += output[1] << 8;
        number += output[2] << 16;
        number += output[3] << 24;
        number += (long)output[8] << 32;
        number += (long)output[9] << 40;
        number += (long)output[10] << 48;
        return number & 0xFFFFFFFFFFFFFFl;
    }

    static int murmur3s(const char* s, const int size) {
        int out;
        MurmurHash3_x86_32(s, size, 0, &out);
        return out;
    }

    static int murmur3(const char* s) {
        int out;
        MurmurHash3_x86_32(s, strlen(s), 0, &out);
        return out;
    }

    static int fnv1a(const char *s) {
        int hval = 0;
        while (*s) {
            hval ^= (int) * s++;
            hval *= 16777619;
        }
        return hval;
    }

    static long fnv1a_56(const char *s, int size) {
        long hval = 0;
        int i = 0;
        while (i < size) {
            hval ^= (int) s[i++];
            hval *= 16777619;
        }
        return hval & 0xFFFFFFFFFFFFFFl;
    }
    static int fnv1as(const char *s, const int size) {
        int hval = 0;
        int i = 0;
        while (i < size) {
            hval ^= (int) s[i++];
            hval *= 16777619;
        }
        return hval;
    }

    static int dbj2(const char* s) {
        unsigned long hash = 5381;
        int c;
        while ((c = *s++))
            hash = ((hash << 5) + hash) + c;
        return hash;
    }

    static int dbj2s(const char* s, const int size) {
        unsigned long hash = 5381;
        int i = 0;
        while (i < size) {
            hash = ((hash << 5) + hash) + s[i++];
        }
        return hash;
    }

    static long dbj2s_56(const char* s, const int size) {
        unsigned long hash = 5381;
        int i = 0;
        while (i < size) {
            hash = ((hash << 5) + hash) + s[i++];
        }
        return hash & 0xFFFFFFFFFFFFFFl;
    }

    static long getCodeWithDefaultFunction(const char *term, const int size) {
        return Hashes::murmur3_56(term, size);
    }

    static size_t hashArray(const uint64_t *array, const size_t size) {
        size_t seed = 0;
        for(size_t i = 0; i < size; ++i) {
            boost::hash_combine(seed,array[i]);
        }
        return seed;
    }
};

#endif /* HASHFUNCTIONS_H_ */
