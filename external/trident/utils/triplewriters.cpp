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

#include <tridentcompr/utils/triplewriters.h>
#include <tridentcompr/sorting/sorter.h>

SimpleTripleWriter::SimpleTripleWriter(string dir, string prefixFile, bool quad) {
    writer = new LZ4Writer(dir + string("/") + prefixFile);
    writer->writeByte(quad);
}

void SimpleTripleWriter::write(const long t1, const long t2, const long t3) {
    writer->writeLong(t1);
    writer->writeLong(t2);
    writer->writeLong(t3);
}

void SimpleTripleWriter::write(const long t1, const long t2, const long t3, const long count) {
    writer->writeLong(t1);
    writer->writeLong(t2);
    writer->writeLong(t3);
    writer->writeLong(count);
}

SortedTripleWriter::SortedTripleWriter(string dir, string prefixFile,
                                       int fileSize) :
    fileSize(fileSize) {
    buffersCurrentSize = 0;
    idLastWrittenFile = -1;
    this->dir = dir;
    this->prefixFile = prefixFile;
    buffer.clear();
}

void SortedTripleWriter::write(const long t1, const long t2, const long t3) {
    write(t1, t2, t3, 0);
}

void SortedTripleWriter::write(const long t1, const long t2, const long t3, const long count) {
    Triple t;
    t.s = t1;
    t.p = t2;
    t.o = t3;
    t.count = count;
    buffer.push_back(t);
    buffersCurrentSize++;

    if (buffersCurrentSize == fileSize) {
        idLastWrittenFile++;
        string fileName = dir + string("/") + prefixFile
                          + to_string(idLastWrittenFile);
        Sorter::sortBufferAndWriteToFile(buffer, fileName);
        buffersCurrentSize = 0;
        buffer.clear();
    }
}

SortedTripleWriter::~SortedTripleWriter() {
    if (buffersCurrentSize > 0) {
        idLastWrittenFile++;
        string fileName = dir + string("/") + prefixFile
                          + to_string(idLastWrittenFile);
        Sorter::sortBufferAndWriteToFile(buffer, fileName);
    }
}


