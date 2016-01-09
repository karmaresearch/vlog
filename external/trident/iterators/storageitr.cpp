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

#include <trident/iterators/storageitr.h>
#include <trident/storage/pairhandler.h>

#include <iostream>

using namespace std;

bool StorageItr::has_next() {
    if (!hasNextChecked && next) {
        // Do the next and calculate the value
        if (pair->more_data()) {
            pair->next_pair();
            next = (constraint1 == -1
                    || (constraint1 == pair->value1()
                        && (constraint2 == -1
                            || constraint2 == pair->value2())));
        } else {
            next = false;
        }
        hasNextChecked = true;
    }
    return next;
}

void StorageItr::next_pair() {
    hasNextChecked = false;
}

void StorageItr::mark() {
    pair->mark();
    markHasNextChecked = hasNextChecked;
    markNext = next;
}

void StorageItr::reset(const char i) {
    pair->reset();
    hasNextChecked = markHasNextChecked;
    next = markNext;
}

void StorageItr::move_first_term(long c1) {
    pair->moveToClosestFirstTerm(c1);
    if (constraint1 == -1)
        next = pair->value1() >= c1;
    else
        next = c1 == pair->value1();
    hasNextChecked = true;
}

uint64_t StorageItr::getCard() {
    if (next) {
        return pair->estimateNPairs();
    } else {
        return 0;
    }
}

void StorageItr::move_second_term(long c2) {
    pair->moveToClosestSecondTerm(pair->value1(), c2);
    next = (constraint1 == -1
            || constraint1 == pair->value1());
    if (next) {
        if (constraint2 == -1) {
            next = (pair->value2() >= c2);
        } else {
            next = (pair->value2() == c2);
        }
    }
    hasNextChecked = true;
}

void StorageItr::init(TableStorage *storage, const short file, const int mark,
                      PairHandler *pair, const long constraint1, const long constraint2) {
    this->pair = pair;
    this->constraint1 = constraint1;
    this->constraint2 = constraint2;
    storage->setupPairHandler(pair, file, mark);
    if (constraint1 != -1) {
        pair->moveToClosestFirstTerm(constraint1);
        // Did we actually find the element?
        if (pair->value1() == constraint1 && constraint2 != -1) {
            pair->moveToClosestSecondTerm(constraint1, constraint2);
        }
    } else {
        pair->next_pair();
    }
    next = (constraint1 == -1
            || (constraint1 == pair->value1()
                && (constraint2 == -1 || constraint2 == pair->value2())));
    hasNextChecked = true;
}

void StorageItr::ignoreSecondColumn() {
    pair->ignoreSecondColumn();
}

void StorageItr::clear() {
    pair->clear();
    pair = NULL;
}
