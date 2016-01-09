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

#ifndef FLA_H
#define FLA_H

#include <cmath>
#include <vector>

using namespace std;

#define FLAJETCOS 0.77531

class FlajoletMartin {
private:

    long key1, key2, key3;

    //Taken from http://graphics.stanford.edu/~seander/bithacks.html#ZerosOnRightMultLookup
    static int deBruijnAlgo(const unsigned int v) {
        int r;           // result goes here
        static const int MultiplyDeBruijnBitPosition[32] = {
            0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
            31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
        };
        r = MultiplyDeBruijnBitPosition[((uint32_t)((v & -v) * 0x077CB531U)) >> 27];
        return r;
    }

public:

    FlajoletMartin() {
        key1 = key2 = key3 = 0;
    }

    static int posleastSignificantOne(const long i) {
        unsigned int n = (unsigned int)i;
        if (n == 0) {
            n = (unsigned int)(i >> 32);
            int result = deBruijnAlgo(n);
            if (result == 0) {
                result = 64;
            } else {
                result += 32;
            }
            return result;
        } else {
            return deBruijnAlgo(n);
        }
    }

    static int posFirstZero(long n) {
        int i = 0;
        for (; i < 64 && n & 1; ++i)
            n >>= 1;
        return i;
    }

    void addElement(const long el1, const long el2, const long el3) {
        int p1 = posleastSignificantOne(el1);
        int p2 = posleastSignificantOne(el2);
        int p3 = posleastSignificantOne(el3);
        key1 |= (long)1 << p1;
        key2 |= (long)1 << p2;
        key3 |= (long)1 << p3;
    }

    long estimateCardinality() {
        int pos1 = posFirstZero(key1);
        int pos2 = posFirstZero(key2);
        int pos3 = posFirstZero(key3);
        int avg = (pos1 + pos2 + pos3) / 3;
        return (long) (double)pow(2, avg) / FLAJETCOS * 3;
    }
};

#endif
