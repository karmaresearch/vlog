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

#ifndef STRINGSCOL_H_
#define STRINGSCOL_H_

#include <vector>

class StringCollection {
private:
	const int segmentSize;
	int currentPos;
	int currentIdx;
	std::vector<char *> pool;

public:
	StringCollection(int segmentSize) :
			segmentSize(segmentSize) {
		currentPos = 0;
		currentIdx = 0;
		char *newsegment = new char[segmentSize];
		pool.push_back(newsegment);
	}

	void clear();

    void deallocate();

	const char *addNew(const char *text, int size);

	long allocatedBytes();

    long occupiedBytes();

	~StringCollection();
};

#endif /* STRINGSCOL_H_ */
