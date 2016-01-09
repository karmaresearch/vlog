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

#ifndef ARRAYITR_H_
#define ARRAYITR_H_

#include <trident/iterators/pairitr.h>

#include <inttypes.h>

typedef std::vector<std::pair<uint64_t, uint64_t> > Pairs;

class ArrayItr: public PairItr {
private:
    Pairs *array;
    int nElements;
    int pos;
    long v1, v2;

    int markPos;
    bool hasNextChecked;
    bool next;

    static int binarySearch(Pairs *array, int end, uint64_t key);

public:
    int getType() {
        return 1;
    }

    long getValue1() {
        return v1;
    }

    long getValue2() {
        return v2;
    }

    uint64_t getCard() {
        return nElements - pos;
    }

    bool allowMerge() {
        return true;
    }

    bool has_next();

    void next_pair();

    void mark();

    void reset(const char i);

    void clear();

    void move_first_term(long c1);

    void move_second_term(long c2);

    void init(Pairs* values, int64_t v1, int64_t v2);
};

#endif /* ARRAYITR_H_ */

