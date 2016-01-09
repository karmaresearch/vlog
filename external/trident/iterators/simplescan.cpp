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

#include <trident/iterators/simplescanitr.h>
#include <trident/tree/treeitr.h>
#include <trident/kb/querier.h>

#include <tridentcompr/utils/lz4io.h>

#include <iostream>

using namespace std;

void SimpleScanItr::init(Querier *q) {
    this->q = q;
    reader = new LZ4Reader(q->getPathRawData());
}

uint64_t SimpleScanItr::getCard() {
    return q->getInputSize();
}

bool SimpleScanItr::has_next() {
    return !reader->isEof();
}

void SimpleScanItr::next_pair() {
    setKey(reader->parseVLong());
    v1 = reader->parseVLong();
    v2 = reader->parseVLong();
}

void SimpleScanItr::clear() {
    delete reader;
}

void SimpleScanItr::mark() {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}

void SimpleScanItr::reset(const char i) {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}

void SimpleScanItr::move_first_term(long c1) {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}

void SimpleScanItr::move_second_term(long c2) {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}
