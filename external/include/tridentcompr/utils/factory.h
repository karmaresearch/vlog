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

#ifndef FACTORY_H_
#define FACTORY_H_

#include <iostream>

#define DEFAULT_SIZE 1000

template<class TypeEl>
class Factory {
private:
	TypeEl **elements;
	int maxSize;
	int size;

public:
	Factory() :
			maxSize(DEFAULT_SIZE) {
		elements = new TypeEl*[maxSize];
		size = 0;
	}

	Factory(int maxSize) :
			maxSize(maxSize) {
		elements = new TypeEl*[maxSize];
		size = 0;
	}

	TypeEl *get() {
		if (size > 0) {
			return elements[--size];
		} else {
			return new TypeEl();
		}
	}

	void release(TypeEl *el) {
		if (size < maxSize) {
			elements[size++] = el;
		} else {
			delete el;
		}
	}

	~Factory() {
		for(int i = 0; i < size; ++i) {
			delete elements[i];
		}
		delete[] elements;
	}
};

template<class TypeEl>
class PreallocatedFactory {
private:
	const int maxPreallocatedSize;
	int preallocatedSize;
	TypeEl *preallocatedElements;

	const int maxSize;
	int size;
	TypeEl **elements;

public:
	PreallocatedFactory(int maxSize, int preallocatedSize) :
			maxPreallocatedSize(preallocatedSize), maxSize(maxSize) {
		this->size = 0;
		elements = new TypeEl*[maxSize];
		this->preallocatedSize = preallocatedSize;
		preallocatedElements = new TypeEl[preallocatedSize];
	}

	void release(TypeEl *el) {
		if (size < maxSize) {
			elements[size++] = el;
		} else {
			deallocate(el);
		}
	}

	void deallocate(TypeEl *el) {
		if (el < preallocatedElements
				|| el >= preallocatedElements + maxPreallocatedSize) {
			delete el;
		}
	}

	TypeEl *get() {
		if (preallocatedSize > 0) {
			preallocatedSize--;
			return preallocatedElements + preallocatedSize;
		} else {
			return new TypeEl;
		}
	}

	~PreallocatedFactory() {
		for (int i = 0; i < size; ++i) {
			deallocate(elements[i]);
		}
		delete[] elements;
		delete[] preallocatedElements;
	}
};

template<class TypeEl>
class PreallocatedArraysFactory {
private:
	const int arraySize;

	const int maxSize;
	int size;
	TypeEl **elements;

	const int maxPreallocatedSize;
	int preallocatedSize;
	TypeEl *preallocatedElements;

public:
	PreallocatedArraysFactory(int arraySize, int maxSize, int preallocatedSize) :
			arraySize(arraySize), maxSize(maxSize), maxPreallocatedSize(
					preallocatedSize * arraySize) {
		this->size = 0;
		elements = new TypeEl*[maxSize];
		this->preallocatedSize = preallocatedSize;
		preallocatedElements = new TypeEl[maxPreallocatedSize]();
	}

	void release(TypeEl *el) {
		if (size < maxSize) {
			elements[size++] = el;
		} else {
			deallocate(el);
		}
	}

	TypeEl *get() {
		if (preallocatedSize > 0) {
			preallocatedSize--;
			return preallocatedElements + (preallocatedSize * arraySize);
		} else {
			return new TypeEl[arraySize]();
		}
	}

	void deallocate(TypeEl *el) {
		if (el < preallocatedElements
				|| el >= preallocatedElements + maxPreallocatedSize) {
			delete[] el;
		}
	}

	~PreallocatedArraysFactory() {
		for (int i = 0; i < size; ++i) {
			deallocate(elements[i]);
		}
		delete[] elements;
		delete[] preallocatedElements;
	}
};

#endif
