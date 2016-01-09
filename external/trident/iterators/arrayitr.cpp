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

#include <trident/iterators/arrayitr.h>

#include <iostream>

using namespace std;

bool ArrayItr::has_next() {

    if (hasNextChecked) {
        return next;
    }

    if (constraint1 == -1) {
        next = pos < nElements;
    } else {
        // Check whether the elements in pos1 and pos2 satisfy the
        // constraints
        if (pos == nElements || array->at(pos).first != constraint1
                || (constraint2 != -1 && array->at(pos).second != constraint2)) {
            next = false;
        } else {
            next = true;
        }
    }
    hasNextChecked = true;
    return next;
}

void ArrayItr::next_pair() {
    v1 = array->at(pos).first;
    v2 = array->at(pos).second;
    pos++;
    hasNextChecked = false;
}

void ArrayItr::clear() {
    array = NULL;
}

void ArrayItr::mark() {
    markPos = pos;
}

void ArrayItr::reset(const char i) {
    pos = markPos;
    hasNextChecked = false;
}

void ArrayItr::move_first_term(long c1) {
    pos = binarySearch(array, nElements, c1);
    if (pos < 0) {
        pos = 0;
    }
    hasNextChecked = false;
}

void ArrayItr::move_second_term(long c2) {
    //Simple solution: Linear search
    while (pos < nElements  && array->at(pos).first == constraint1 && array->at(pos).second < c2) {
        pos++;
    }
    hasNextChecked = false;
}

int ArrayItr::binarySearch(Pairs *array, int end, uint64_t key) {
    int low = 0;
    int high = end - 1;

    while (low <= high) {
        int mid = (low + high) >> 1;
        long midVal = array->at(mid).first;

        if (midVal < key)
            low = mid + 1;
        else if (midVal > key)
            high = mid - 1;
        else if (low != mid) // Equal but range is not fully scanned
            high = mid; // Set upper bound to current number and rescan
        else
            // Equal and full range is scanned
            return mid;
    }

    if (low >= end) {
        return -low - 1;
    } else {
        return high;
    }
}

void ArrayItr::init(Pairs *values, int64_t v1, int64_t v2) {
    this->array = values;
    nElements = (int)values->size();
    constraint1 = v1;
    constraint2 = v2;
    hasNextChecked = false;
    this->v1 = this->v2 = -1;

    if (constraint1 != -1) {
        pos = binarySearch(array, nElements, constraint1);
        if (pos >= 0) {
            if (constraint2 != -1) {
                // The values are not many. Let's try to do a linear search
                // and see how it goes...
                while (pos < nElements && array->at(pos).first == constraint1
                        && array->at(pos).second < constraint2) {
                    pos++;
                }
                hasNextChecked = true;
                next = pos < nElements && array->at(pos).second == constraint2;
            } else {
                hasNextChecked = next = true;
            }
        } else {
            hasNextChecked = true;
            next = false;
            pos = 0;
        }
    } else {
        pos = 0;
        hasNextChecked = next = true;
    }
}

