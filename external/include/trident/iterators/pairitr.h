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

#ifndef PAIRITR_H_
#define PAIRITR_H_

#include <trident/storage/pairhandler.h>
#include <trident/storage/pairstorage.h>

#include <inttypes.h>

class PairHandler;

#define NO_CONSTRAINT -1

class PairItr {
protected:
    long constraint1;
    long constraint2;
    long key;
public:
    virtual int getType() = 0;

    virtual long getValue1() = 0;

    virtual long getValue2() = 0;

    virtual bool has_next() = 0;

    virtual void next_pair() = 0;

    virtual void clear() = 0;

    virtual void move_first_term(long c1) = 0;

    virtual void move_second_term(long c2) = 0;

    virtual void mark() = 0;

    virtual uint64_t getCard() = 0;

    virtual void reset(const char i) = 0;

    virtual void ignoreSecondColumn() {
        throw 10; //Default behaviour: not supported
    }

    virtual PairHandler *getPairHandler() {
        return NULL;
    }

    long getKey() {
        return key;
    }

    void setKey(long key) {
        this->key = key;
    }

    long getConstraint2() {
        return constraint2;
    }

    virtual void set_constraint2(const long c2) {
        constraint2 = c2;
    }

    long getConstraint1() {
        return constraint1;
    }

    virtual void set_constraint1(const long c1) {
        constraint1 = c1;
    }

    virtual bool allowMerge() = 0;

    virtual ~PairItr() {
    }
};

#endif
