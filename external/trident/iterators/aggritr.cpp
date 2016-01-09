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

#include <trident/iterators/aggritr.h>
#include <trident/kb/querier.h>
#include <iostream>

using namespace std;

void AggrItr::setup_second_itr(const int idx) {
    long coordinates = mainItr->getValue2();
    secondItr = q->getFilePairIterator(idx, p, strategy(coordinates),
                                       file(coordinates), pos(coordinates));
    secondItr->mark();
}

bool AggrItr::has_next() {
    if (!hasNextChecked) {
        if (secondItr == NULL) {
            if (mainItr->has_next()) {
                mainItr->next_pair();
                setup_second_itr(idx);
                next = secondItr->has_next();
                secondItr->next_pair();
            } else {
                next = false;
            }
        } else {
            next = secondItr->has_next();
            if (next) {
                secondItr->next_pair();
            } else if (constraint1 == NO_CONSTRAINT) {
                q->releaseItr(secondItr);
                secondItr = NULL;
                if (mainItr->has_next()) {
                    mainItr->next_pair();
                    setup_second_itr(idx);
                    next = secondItr->has_next();
                    if (next) {
                        secondItr->next_pair();
                    }
                } else {
                    next = false;
                }
            }
        }
        hasNextChecked = true;
    }
    return next;
}

void AggrItr::next_pair() {
    hasNextChecked = false;
}

void AggrItr::mark() {
    mainItr->mark();
}

void AggrItr::reset(const char i) {
    if (i == 0) {
        mainItr->reset(i);
        if (secondItr != NULL) {
            q->releaseItr(secondItr);
            secondItr = NULL;
        }
    } else {
        if (secondItr != NULL)
            secondItr->reset(i);
    }
    hasNextChecked = false;
}

void AggrItr::clear() {
    mainItr = NULL;
    secondItr = NULL;
}

void AggrItr::move_first_term(long c1) {
    mainItr->move_first_term(c1);
    hasNextChecked = true;
    next = mainItr->has_next();
    if (next) {
        mainItr->next_pair();
        if (secondItr != NULL) {
            q->releaseItr(secondItr);
        }
        setup_second_itr(idx);
        next = secondItr->has_next();
        if (next)
            secondItr->next_pair();
    }
}

void AggrItr::move_second_term(long c2) {
    secondItr->move_second_term(c2);
    hasNextChecked = true;
    next = secondItr->has_next();
    if (next)
        secondItr->next_pair();
}

void AggrItr::init(long p, int idx, PairItr* itr, Querier *q) {
    this->p = p;
    mainItr = itr;
    this->q = q;
    this->idx = idx;
    hasNextChecked = false;
    PairItr::set_constraint1(mainItr->getConstraint1());
    PairItr::set_constraint2(mainItr->getConstraint2());
}

uint64_t AggrItr::getCard() {
    if (next) {
        uint64_t cardSec = secondItr->getCard();
        if (getConstraint1() >= 0) {
            return cardSec;
        } else {
            uint64_t cardFirst = mainItr->getCard();
            return cardFirst * cardSec;
        }
    } else {
        return 0;
    }
}
