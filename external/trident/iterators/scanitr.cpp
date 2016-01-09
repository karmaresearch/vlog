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

#include <trident/iterators/scanitr.h>
#include <trident/tree/treeitr.h>
#include <trident/kb/querier.h>

#include <iostream>

using namespace std;

void ScanItr::init(TreeItr *itr, Querier *q) {
    this->itr = itr;
    this->q = q;
    currentItr = NULL;
    hasNextChecked = false;
}

uint64_t ScanItr::getCard() {
    return q->getInputSize();
}

bool ScanItr::has_next() {
    if (!hasNextChecked) {
        if (currentItr != NULL && currentItr->has_next()) {
            hasNext = true;
        } else {

            //move to the following key
            if (currentItr != NULL)
                q->releaseItr(currentItr);

            if (itr->hasNext()) {
                currentKey = itr->next(&currentValue);
                if (!currentValue.exists(0)) {
                    bool ok = false;
                    while (itr->hasNext()) {
                        currentKey = itr->next(&currentValue);
                        if (currentValue.exists(0)) {
                            ok = true;
                            break;
                        }
                    }
                    if (!ok) {
                        hasNext = false;
                        hasNextChecked = true;
                        return hasNext;
                    }
                }
                currentItr = q->getPairIterator(&currentValue, 0, -1, -1);
                hasNext = currentItr->has_next();
            } else {
                hasNext = false;
            }
        }
        hasNextChecked = true;
    }
    return hasNext;
}

void ScanItr::next_pair() {
    currentItr->next_pair();
    v1 = currentItr->getValue1();
    v2 = currentItr->getValue2();
    setKey(currentKey);
    hasNextChecked = false;
}

void ScanItr::clear() {
    if (currentItr != NULL) {
        q->releaseItr(currentItr);
    }
    delete itr;
}

void ScanItr::mark() {
}

void ScanItr::reset(const char i) {
}

void ScanItr::move_first_term(long c1) {
    cerr << "scanitr: not implemented" << endl;
}

void ScanItr::move_second_term(long c2) {
    cerr << "scanitr: not implemented" << endl;
}

