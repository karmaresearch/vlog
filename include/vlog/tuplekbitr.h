/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _TUPLEKB_ITR
#define _TUPLEKB_ITR

#include <trident/iterators/tupleiterators.h>
#include <trident/iterators/pairitr.h>

class Tuple;
class Querier;
class TupleKBItr : public TupleIterator {
private:
    Querier *querier;
    PairItr *physIterator;
    int idx;
    int *invPerm;

    bool onlyVars;
    uint8_t varsPos[3];
    uint8_t sizeTuple;

    std::vector<std::pair<uint8_t, uint8_t>> equalFields;

    bool nextProcessed;
    bool nextOutcome;
    size_t processedValues;

    bool checkFields();

public:
    TupleKBItr();

    PairItr *getPhysicalIterator() {
        return physIterator;
    }

    void ignoreSecondColumn() {
        physIterator->ignoreSecondColumn();
    }

    void init(Querier *querier, const Tuple *literal,
              const std::vector<uint8_t> *fieldsToSort) {
        init(querier, literal, fieldsToSort, false);
    }

    void init(Querier *querier, const Tuple *literal,
              const std::vector<uint8_t> *fieldsToSort, bool onlyVars);

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);	// No Term_t: overrides method in trident

    ~TupleKBItr();

    void clear();
};

#endif
