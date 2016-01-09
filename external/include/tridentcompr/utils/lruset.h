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

#ifndef LRUSET_H_
#define LRUSET_H_

#include <iostream>
#include <boost/log/trivial.hpp>

class LRUSet {
private:
	char **cache;
	int *sizeCacheElements;

	int *hashCache;
	const int size;
public:
	LRUSet(int size) :
			size(size) {
		cache = new char*[size];
		sizeCacheElements = new int[size];
		hashCache = new int[size];
		for (int i = 0; i < size; ++i) {
			cache[i] = NULL;
			sizeCacheElements[i] = 0;
		}
	}

	bool exists(const char *term, int s) {
		int hash = abs(Hashes::dbj2s(term, s));
		int idx = hash % size;
		if (cache[idx] != NULL && hashCache[idx] == hash
				&& sizeCacheElements[idx] >= s + 2
				&& Utils::decode_short(cache[idx], 0) == s) {
			return memcmp(cache[idx] + 2, term, s - 2) == 0;
		}
		return false;
	}

	bool exists(const char *term) {
		int s = Utils::decode_short((char*) term);
		int hash = abs(Hashes::dbj2s(term + 2, s));
		int idx = hash % size;
		if (cache[idx] != NULL && hashCache[idx] == hash
				&& sizeCacheElements[idx] >= s + 2) {
			int s1 = Utils::decode_short(cache[idx]);
			if (s == s1) {
				return memcmp(cache[idx] + 2, term + 2, s) == 0;
			}
		}
		return false;
	}

	void add(const char* term) {
		int s = Utils::decode_short((char*) term, 0);
		int hash = abs(Hashes::dbj2s(term + 2, s));
		int idx = hash % size;

		if (cache[idx] == NULL) {
			cache[idx] = new char[MAX_TERM_SIZE];
		}

		if (sizeCacheElements[idx] < s + 2) {
			delete[] cache[idx];
			cache[idx] = new char[s + 2];
			sizeCacheElements[idx] = s + 2;
		}

		memcpy(cache[idx], term, s + 2);
		hashCache[idx] = hash;
	}

	void add(const char* term, const int s) {
		int hash = abs(Hashes::dbj2s(term, s));
		int idx = hash % size;

		if (cache[idx] == NULL) {
			cache[idx] = new char[s + 2];
			sizeCacheElements[idx] = s + 2;
		}

		if (sizeCacheElements[idx] < s + 2) {
			delete[] cache[idx];
			cache[idx] = new char[s + 2];
			sizeCacheElements[idx] = s + 2;
		}

		Utils::encode_short(cache[idx], 0, s);
		memcpy(cache[idx] + 2, term, s);
		hashCache[idx] = hash;
	}

	~LRUSet() {
		for (int i = 0; i < size; ++i) {
			if (cache[i] != NULL)
				delete[] cache[i];
		}
		delete[] cache;
		delete[] sizeCacheElements;
		delete[] hashCache;
	}
};

#endif /* LRUSET_H_ */
