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

#ifndef POSITR_H_
#define POSITR_H_

#include <trident/iterators/pairitr.h>

class Querier;

class AggrItr: public PairItr {
private:
    Querier *q;
    PairItr *mainItr;
    PairItr *secondItr;

    bool hasNextChecked, next;
    int idx;

    long p;

    char strategy(long coordinates) {
        return (char) ((coordinates >> 48) & 0xFF);
    }

    short file(long coordinates) {
        return (short)((coordinates >> 32) & 0xFFFF);
    }

    int pos(long coordinates) {
        return (int) coordinates;
    }

    void setup_second_itr(const int idx);

public:
    int getType() {
        return 4;
    }

    long getValue1() {
        return mainItr->getValue1();
    }

    long getValue2() {
        return secondItr->getValue2();
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

    void init(long p, int idx, PairItr* itr, Querier *q);

    uint64_t getCard();

    void set_constraint1(const long c1) {
        PairItr::set_constraint1(c1);
        mainItr->set_constraint1(c1);
    }

    void set_constraint2(const long c2) {
        PairItr::set_constraint2(c2);
        secondItr->set_constraint2(c2);
    }

    PairItr *getMainItr() {
        return mainItr;
    }

    PairItr *getSecondItr() {
        return secondItr;
    }
};

#endif
