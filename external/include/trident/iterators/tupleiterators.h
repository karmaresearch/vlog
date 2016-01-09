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

#ifndef _TUPLE_ITR_H
#define _TUPLE_ITR_H

#include <cstddef>
#include <inttypes.h>
#include <vector>
#include <cstdlib>

class TupleIterator {
public:
    virtual bool hasNext() = 0;

    virtual void next() = 0;

    virtual size_t getTupleSize() = 0;

    virtual uint64_t getElementAt(const int pos) = 0;

    virtual ~TupleIterator() {}
};
#endif
