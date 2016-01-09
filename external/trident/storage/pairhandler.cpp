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

#include <trident/storage/pairhandler.h>

#include <tridentcompr/utils/utils.h>

#include <iostream>

using namespace std;

PairHandler::PairHandler() {
    markValue1 = markValue2 = 0;
    markPos = 0;
    basePos = b_start = b_current = b_length = 0;
    markFile = baseFile = 0;
    manager = NULL;
    b = NULL;
    index = NULL;

    currentValue1 = currentValue2 = 0;
    currentPos = limitPos = 0;
    currentFile = limitFile = 0;
    sessionId = EMPTY_SESSION;
}

void PairHandler::init(FileManager<ComprFileDescriptor, FileSegment> *manager,
                       FileIndex * idx) {
    this->manager = manager;
    this->index = idx;
    currentFile = -1;

    //Get a new sessionId
    sessionId = manager->newSession();
}

long PairHandler::getNBytesFrom(short file, int pos) {
    assert(getCurrentFile() >= file);
    if (getCurrentFile() == file) {
        assert(getCurrentPosition() >= pos);
        return getCurrentPosition() - pos;
    } else {
        long diff = getCurrentPosition();
        short f = getCurrentFile();
        f--;
        while (f > file) {
            diff += manager->sizeFile(f);
            f--;
        }
        diff += manager->sizeFile(f) - pos;
        return diff;
    }
}

void PairHandler::clear() {
    if (sessionId != EMPTY_SESSION) {
	manager->closeSession(sessionId);
    }
    sessionId = EMPTY_SESSION;
}

void PairHandler::setEndChunk() {
    currentFile = limitFile;
    currentPos = limitPos;
}

bool PairHandler::more_data() {
    return currentFile < limitFile
           || (currentFile == limitFile && currentPos < limitPos);
}

void PairHandler::setBasePosition(short file, int pos) {
    this->baseFile = file;
    this->basePos = pos;
}

void PairHandler::setLimitPosition(short file, int pos) {
    this->limitFile = file;
    this->limitPos = pos;
}

void PairHandler::setPosition(const short file, const int pos) {
    if (currentFile != file) {
        b_length = BUFFER_SIZE;
        b = manager->getBuffer(file, pos, &b_length, sessionId);
        b_current = 0;
        b_start = currentPos = pos;
        currentFile = file;
    } else {
        this->setPosition(pos);
    }
}

void PairHandler::setPosition(const int pos) {
    if (b == NULL || (pos + BLOCK_MIN_SIZE) > b_start + b_length
            || pos < b_start) {
        b_length = BUFFER_SIZE;
        b = manager->getBuffer(currentFile, pos, &b_length, sessionId);
        b_start = currentPos = pos;
        b_current = 0;
    } else {
        currentPos = pos;
        b_current = pos - b_start;
    }
}

void PairHandler::setRelativePosition(short file, int position) {
    if (file != baseFile) {
        setPosition(file, position);
    } else {
        setPosition(file, position + basePos);
    }
}

void PairHandler::appendPair(const long t1, const long t2) {
    //Check if there is enough space
    if (currentPos + 17 > manager->getFileMaxSize()) {
        currentFile = manager->createNewFile();
        currentPos = 0;
    }
    append(t1, t2);
}

void PairHandler::mark() {
    markFile = currentFile;
    markPos = currentPos;
    markValue1 = currentValue1;
    markValue2 = currentValue2;
}

void PairHandler::reset() {
    currentFile = markFile;
    currentPos = markPos;
    currentValue1 = markValue1;
    currentValue2 = markValue2;
    //Is the buffer resetted?
    setPosition(currentFile, currentPos);
}

long PairHandler::value1() {
    return currentValue1;
}

long PairHandler::value2() {
    return currentValue2;
}

void PairHandler::advance() {
    // Check if the current buffer is finished. If so, reads the next chunk.
    // If the chunk is finished, then move to the next file
    if (b_length - b_current < BLOCK_MIN_SIZE) {
        if (manager->sizeFile(currentFile) > currentPos) {
            b_length = BUFFER_SIZE;
            b = manager->getBuffer(currentFile, currentPos, &b_length, sessionId);
            b_current = 0;
            b_start = currentPos;
        } else {
            // Move to the following file
            currentFile++;
            if (currentFile <= manager->getIdLastFile()) {
                b_start = b_current = currentPos = 0;
                b_length = BUFFER_SIZE;
                b = manager->getBuffer(currentFile, currentPos, &b_length, sessionId);
            }
        }
    }
}

long PairHandler::getVLong() {
    int b = b_current;
    long n = Utils::decode_vlong(this->b, &b_current);
    currentPos += b_current - b;
    return n;
}

long PairHandler::getVLong2() {
    int b = b_current;
    long n = Utils::decode_vlong2(this->b, &b_current);
    currentPos += b_current - b;
    return n;
}

long PairHandler::getLong() {
    long n = Utils::decode_long(this->b, b_current);
    b_current += 8;
    currentPos += 8;
    return n;
}

int PairHandler::getByte() {
    currentPos++;
    return b[b_current++];
}

short PairHandler::getCurrentFile() {
    return currentFile;
}

int PairHandler::getCurrentPosition() {
    return currentPos;
}

short PairHandler::getLimitFile() {
    return limitFile;
}

int PairHandler::getLimitPosition() {
    return limitPos;
}

void PairHandler::setup(FileManager<ComprFileDescriptor, FileSegment> *manager, short file,
                        int p, FileIndex *index) {
    this->manager = manager;
    this->index = index;
    setPosition(file, p);
}

void PairHandler::cleanup() {
    b = NULL;
}

int PairHandler::writeVLong2(long t) {
    int prevPos = currentPos;
    currentPos = manager->appendVLong2(t);
    return currentPos - prevPos;
}

void PairHandler::overwriteVLong2(short file, int pos, long number) {
    manager->overwriteVLong2At(file, pos, number);
}

//reserve "bytes" consecutive bytes
void PairHandler::reserveBytes(const uint8_t bytes) {
    manager->reserveBytes(bytes);
    currentPos += bytes;
}

void PairHandler::createNewFileIfCurrentIsTooLarge() {
    if (manager->sizeLastFile() + 17 >= manager->getFileMaxSize()) {
        currentFile = manager->createNewFile();
        currentPos = 0;
    }
}

//uint64_t PairHandler::getTotalBytes() {
//    if (currentFile == limitFile) {
//        return limitPos - currentPos;
//    } else {
//        uint64_t size = 0;
//        for(int i = currentFile; i < limitFile; ++i)
//            size += manager->sizeFile(i);
//        return limitPos + size;
//    }
//}
