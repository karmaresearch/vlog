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

#ifndef FILEMERGER_H_
#define FILEMERGER_H_

#include "../utils/lz4io.h"
#include "../utils/triple.h"

#include <boost/log/trivial.hpp>

#include <string>
#include <queue>
#include <vector>

using namespace std;

template<class K>
struct QueueEl {
	K key;
	int fileIdx;
};

template<class K>
struct QueueElCmp {
	bool operator()(const QueueEl<K> &t1, const QueueEl<K> &t2) const {
		return t1.key.greater(t2.key);
	}
};

template<class K>
class FileMerger {
private:
	priority_queue<QueueEl<K>, vector<QueueEl<K> >, QueueElCmp<K> > queue;
	LZ4Reader **files;
	int nfiles;
	int nextFileToRead;
	long elementsRead;

public:
	FileMerger(vector<string> fn) {
		//Open the files
		files = new LZ4Reader*[fn.size()];
		nfiles = fn.size();
		elementsRead = 0;
		for (int i = 0; i < fn.size(); ++i) {
			files[i] = new LZ4Reader(fn[i]);
			//Read the first element and put it in the queue
			if (!files[i]->isEof()) {
				QueueEl<K> el;
				el.key.readFrom(files[i]);
				el.fileIdx = i;
				queue.push(el);
				elementsRead++;
			}
		}
		nextFileToRead = -1;
	}

	bool isEmpty() {
		return queue.empty() && nextFileToRead == -1;
	}

	K get() {
		if (nextFileToRead != -1) {
			QueueEl<K> el;
			el.key.readFrom(files[nextFileToRead]);
			el.fileIdx = nextFileToRead;
			queue.push(el);
			elementsRead++;
		}

		//Get the first triple
		QueueEl<K> el = queue.top();
		queue.pop();
		K out = el.key;

		//Replace the current element with a new one from the same file
		if (!files[el.fileIdx]->isEof()) {
			nextFileToRead = el.fileIdx;
		} else {
			nextFileToRead = -1;
		}

		return out;
	}

	~FileMerger() {
		for(int i = 0; i < nfiles; ++i) {
			delete files[i];
		}
		delete[] files;
	}
};

#endif /* FILEMERGER_H_ */
