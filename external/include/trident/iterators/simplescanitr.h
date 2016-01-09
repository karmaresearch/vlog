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

#ifndef SIMPLECANITR_H_
#define SIMPLECANITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/tree/coordinates.h>

#include <tridentcompr/utils/lz4io.h>

class Querier;

class SimpleScanItr: public PairItr {

private:
    Querier *q;
    long v1, v2;
    LZ4Reader *reader;

public:
    void init(Querier *q);

    int getType() {
        return 6;
    }

    long getValue1() {
        return v1;
    }

    long getValue2() {
        return v2;
    }

    bool allowMerge() {
        return true;
    }

    void mark();

    void reset(const char i);

    bool has_next();

    void next_pair();

    void clear();

    uint64_t getCard();

    void move_first_term(long c1);

    void move_second_term(long c2);

};

#endif
