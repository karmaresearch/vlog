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

#include <tridentcompr/utils/stringscol.h>

#include <cstring>

void StringCollection::clear() {
    currentIdx = 0;
    currentPos = 0;
}

void StringCollection::deallocate() {
	for (int i = 0; i < pool.size(); ++i) {
		delete[] pool[i];
	}
    pool.clear();
}

const char *StringCollection::addNew(const char *text, int size) {
    if ((segmentSize - currentPos) < size) {
        //Create new segment.
        if (currentIdx == pool.size() - 1) {
            char *newsegment = new char[segmentSize];
            pool.push_back(newsegment);
        }
        currentIdx++;
        currentPos = 0;
    }
    char *segment = pool[currentIdx];
    int startPos = currentPos;
    memcpy(segment + currentPos, text, size);
//  segment[currentPos + size] = '\0';
    currentPos += size;
    return segment + startPos;
}

long StringCollection::allocatedBytes() {
    return (long)pool.size() * (long)segmentSize;
}

long StringCollection::occupiedBytes() {
    return (long)currentIdx * segmentSize + currentPos;
}

StringCollection::~StringCollection() {
    deallocate();
}
