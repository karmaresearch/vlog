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
        memoryTrackerId = tracker->add(sizeFile, this, id,
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

char* FileDescriptor::getBuffer(int offset, int *length,
                                int &memoryBlock, const int sesID) {
    //sedID is used only by the comprfiledescriptor.
    memoryBlock = memoryTrackerId;
    if (*length > size - offset) {
        *length = size - offset;
    }
    return buffer + offset;
}

int FileDescriptor::appendVLong(const long v) {
    if (8 + this->size > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max(8, (int) sizeFile));
        mapFile(increment);
    }
    const int pos = Utils::encode_vlong(buffer, size, v);
    this->size = pos;
    return pos;
}

int FileDescriptor::appendVLong2(const long v) {
    if (8 + this->size > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max(8, (int) sizeFile));
        mapFile(increment);
    }
    const int pos = Utils::encode_vlong2(buffer, size, v);
    this->size = pos;
    return pos;
}

void FileDescriptor::appendLong(const long v) {
    if (8 + this->size > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max(8, (int) sizeFile));
        mapFile(increment);
    }
    Utils::encode_long(buffer, size, v);
    this->size += 8;
}

void FileDescriptor::appendLong(const uint8_t nbytes, const uint64_t v) {
    if (nbytes + this->size > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max((int)nbytes,
                                 (int) sizeFile));
        mapFile(increment);
    }
    Utils::encode_longNBytes(buffer + size, nbytes, v);
    this->size += nbytes;
}


void FileDescriptor::reserveBytes(const uint8_t n) {
    if (n + this->size > sizeFile) {
        int increment = std::max(SMALLEST_INCR, std::max((int)n, (int) sizeFile));
        mapFile(increment);
    }
    this->size += n;
}

void FileDescriptor::overwriteAt(int pos, char byte) {
    buffer[pos] = byte;
}

void FileDescriptor::overwriteVLong2At(int pos, long number) {
    Utils::encode_vlong2(buffer, pos, number);
}

bool FileDescriptor::isUsed() {
    if (tracker->isUsed(memoryTrackerId))
        return true;
    else
        return false;
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
            fs::resize_file(fs::path(filePath), size);
        }
    }
}
