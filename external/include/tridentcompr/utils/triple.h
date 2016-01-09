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

#ifndef TRIPLE_H_
#define TRIPLE_H_

#include <limits>
#include <functional>

class LZ4Reader;
class LZ4Writer;

typedef struct Triple {
    long s, p, o, count;

    Triple(long s, long p, long o) {
        this->s = s;
        this->p = p;
        this->o = o;
        this->count = 0;
    }

    Triple() {
        s = p = o = count = 0;
    }

    bool less(const Triple &t) const {
        if (s < t.s) {
            return true;
        } else if (s == t.s) {
            if (p < t.p) {
                return true;
            } else if (p == t.p) {
                return o < t.o;
            }
        }
        return false;
    }

    bool greater(const Triple &t) const {
        if (s > t.s) {
            return true;
        } else if (s == t.s) {
            if (p > t.p) {
                return true;
            } else if (p == t.p) {
                return o > t.o;
            }
        }
        return false;
    }

    void readFrom(LZ4Reader *reader);

    void writeTo(LZ4Writer *writer);

} Triple;

const Triple minv(std::numeric_limits<long>::min(),
                  std::numeric_limits<long>::min(), std::numeric_limits<long>::min());
const Triple maxv(std::numeric_limits<long>::max(),
                  std::numeric_limits<long>::max(), std::numeric_limits<long>::max());

struct cmp: std::less<Triple> {
    bool operator ()(const Triple& a, const Triple& b) const {
        if (a.s < b.s) {
            return true;
        } else if (a.s == b.s) {
            if (a.p < b.p) {
                return true;
            } else if (a.p == b.p) {
                return a.o < b.o;
            }
        }
        return false;
    }

    Triple min_value() const {
        return minv;
    }

    Triple max_value() const {
        return maxv;
    }
};

class TripleWriter {
public:
    virtual void write(const long t1, const long t2, const long t3) = 0;
    virtual void write(const long t1, const long t2, const long t3, const long count) = 0;
    virtual ~TripleWriter() {
    }
};

#endif /* TRIPLE_H_ */
