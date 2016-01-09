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

#ifndef STORAGEITR_H_
#define STORAGEITR_H_

#include <trident/iterators/pairitr.h>
#include <trident/storage/pairhandler.h>
#include <trident/storage/pairstorage.h>

#include <boost/chrono.hpp>

namespace timens = boost::chrono;

class StorageItr: public PairItr {
private:
    PairHandler *pair;
    bool hasNextChecked;
    bool next;

    bool markHasNextChecked, markNext;


public:
    int getType() {
        return 0;
    }

    long getValue1() {
        return pair->value1();
    }

    long getValue2() {
        return pair->value2();
    }

    bool allowMerge() {
        return true;
    }

    bool has_next();

    void next_pair();

    void clear();

    void mark();

    uint64_t getCard();

    void reset(const char i);

    void move_first_term(long c1);

    void move_second_term(long c2);

    PairHandler *getPairHandler() {
        return pair;
    }

    void ignoreSecondColumn();

    void init(TableStorage *storage, const short file, const int mark, PairHandler *pair,
              const long constraint1, const long constraint2);
};

#endif /* STORAGEITR_H_ */
