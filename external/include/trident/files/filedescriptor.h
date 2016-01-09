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

#ifndef FILEDESCRIPTOR_H_
#define FILEDESCRIPTOR_H_

#include <trident/memory/memorymgr.h>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <string>

namespace bip = boost::interprocess;

#define SMALLEST_INCR 4*1024

class Stats;
class FileDescriptor {

private:

    const std::string filePath;

    const bool readOnly;

    const int id;

    bip::file_mapping *mapping;

    bip::mapped_region* mapped_rgn;

    char* buffer;

    int size;

    int sizeFile;

    MemoryManager<FileDescriptor> *tracker;
    int memoryTrackerId;
    FileDescriptor **parentArray;

    void mapFile(int requiredIncrement);

public:
    FileDescriptor(bool readOnly, int id, std::string file, int fileMaxSize,
                   MemoryManager<FileDescriptor> *tracker, FileDescriptor **parents,
                   Stats * const stats);

    char* getBuffer(int offset, int *length);

    int getFileLength();

    int getId() {
        return id;
    }

    bool isUsed() {
        return false;
    }

    void shiftFile(int pos, int diff);

    void append(char *bytes, const int size);

    ~FileDescriptor();
};

#endif /* FILEDESCRIPTOR_H_ */

