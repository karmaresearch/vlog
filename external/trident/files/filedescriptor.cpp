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

#include <trident/files/filedescriptor.h>
#include <trident/files/filemanager.h>
#include <trident/kb/consts.h>

#include <tridentcompr/utils/utils.h>

#include <algorithm>
#include <string>
#include <iostream>
#include <fstream>
#include <cmath>

#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>

#include <unistd.h>

using namespace std;

namespace fs = boost::filesystem;

void FileDescriptor::mapFile(int requiredIncrement) {
    if (requiredIncrement > 0) {
        mapped_rgn->flush();
        delete mapped_rgn;
        fs::resize_file(fs::path(filePath), sizeFile + requiredIncrement);
    }
    mapped_rgn = new bip::mapped_region(*mapping,
                                        readOnly ? bip::read_only : bip::read_write);
    buffer = static_cast<char*>(mapped_rgn->get_address());
    sizeFile = (int)mapped_rgn->get_size();

    if (memoryTrackerId != -1) {
        tracker->update(memoryTrackerId, requiredIncrement);
    } else {
        memoryTrackerId = tracker->add(requiredIncrement, this, id,
                                       parentArray);
    }
}

FileDescriptor::FileDescriptor(bool readOnly, int id, std::string file,
                               int fileMaxSize, MemoryManager<FileDescriptor> *tracker,
                               FileDescriptor **parents, Stats * const stats) :
    filePath(file), readOnly(readOnly), id(id), parentArray(parents) {
    this->tracker = tracker;
    memoryTrackerId = -1;

    bool newFile = false;
    if (!readOnly && !fs::exists(file)) {
        ofstream oFile(file);
        oFile.seekp(1024 * 1024);
        oFile.put(0);
        newFile = true;
    }
    mapping = new bip::file_mapping(file.c_str(),
                                    readOnly ? bip::read_only : bip::read_write);
    mapped_rgn = NULL;
    mapFile(0);

    if (newFile) {
        size = 0;
    } else {
        size = sizeFile;
    }
}

char* FileDescriptor::getBuffer(int offset, int *length) {
    if (*length > size - offset) {
        *length = size - offset;
    }
    return buffer + offset;
}

int FileDescriptor::getFileLength() {
    return size;
}

void FileDescriptor::shiftFile(int pos, int diff) {
    if (this->size + diff > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max(diff, (int) sizeFile));
        mapFile(increment);
    }
    memmove(buffer + pos + diff, buffer + pos, size - pos);
}

void FileDescriptor::append(char *bytes, const int size) {
    if (size + this->size > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max(size, (int) sizeFile));
        mapFile(increment);
    }
    memcpy(buffer + this->size, bytes, size);
    this->size += size;
}

FileDescriptor::~FileDescriptor() {
    tracker->removeBlockWithoutDeallocation(memoryTrackerId);

    buffer = NULL;
    if (mapped_rgn != NULL) {
        mapped_rgn->flush();
        delete mapped_rgn;
    }

    delete mapping;

    if (!readOnly) {
        if (size == 0) {
            //Remove the file
            fs::remove(fs::path(filePath));
        } else if (size < sizeFile) {
            //For some reasons this method does not work in linux...
            fs::resize_file(fs::path(filePath), size);
        }
    }
}

