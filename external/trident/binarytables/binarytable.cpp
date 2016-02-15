#include <trident/binarytables/binarytable.h>

#include <tridentcompr/utils/utils.h>

#include <iostream>

using namespace std;

BinaryTable::BinaryTable() {
    //markValue1 = markValue2 = 0;
    //markPos = 0;
    basePos = b_start = b_current = b_length = 0;
    //markFile =
    baseFile = 0;
    manager = NULL;
    b = NULL;
    index = NULL;
    hasNextChecked = false;
    hasNextFlag = false;
    nextDone = false;

    currentValue1 = currentValue2 = 0;
    currentPos = limitPos = 0;
    currentFile = limitFile = 0;
    sessionId = EMPTY_SESSION;
}

void BinaryTable::init(FileManager<FileDescriptor, FileDescriptor> *manager,
                       FileIndex * idx) {
    this->manager = manager;
    this->index = idx;
    currentFile = -1;
    hasNextChecked = false;
    hasNextFlag = false;
    nextDone = false;
    secondColumnIgnored = false;

    //Get a new sessionId
    sessionId = manager->newSession();
}

void BinaryTable::clear() {
    if (sessionId != EMPTY_SESSION) {
        manager->closeSession(sessionId);
    }
    sessionId = EMPTY_SESSION;
}

void BinaryTable::setEndChunk() {
    currentFile = limitFile;
    currentPos = limitPos;
}

bool BinaryTable::has_next() {
    return currentFile < limitFile
           || (currentFile == limitFile && currentPos < limitPos);
}

void BinaryTable::setBasePosition(short file, int pos) {
    this->baseFile = file;
    this->basePos = pos;
}

void BinaryTable::setLimitPosition(short file, int pos) {
    this->limitFile = file;
    this->limitPos = pos;
}

void BinaryTable::setPosition(const short file, const int pos) {
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

void BinaryTable::setPosition(const int pos) {
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

void BinaryTable::setP1() {
    b_P1 = b + b_current;
}

void BinaryTable::setRelativePosition(short file, int position) {
    if (file != baseFile) {
        setPosition(file, position);
    } else {
        setPosition(file, position + basePos);
    }
}

long BinaryTable::getValue1() {
    return currentValue1;
}

long BinaryTable::getValue2() {
    return currentValue2;
}

void BinaryTable::advance() {
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

long BinaryTable::getVLong() {
    int b = b_current;
    long n = Utils::decode_vlong(this->b, &b_current);
    currentPos += b_current - b;
    return n;
}

long BinaryTable::getVLong2() {
    int b = b_current;
    long n = Utils::decode_vlong2(this->b, &b_current);
    currentPos += b_current - b;
    return n;
}

long BinaryTable::getLong() {
    long n = Utils::decode_long(this->b, b_current);
    b_current += 8;
    currentPos += 8;
    return n;
}

long BinaryTable::getLong(const uint8_t nbytes) {
    long n = Utils::decode_longFixedBytes(this->b + b_current, nbytes);
    b_current += nbytes;
    currentPos += nbytes;
    return n;
}

long BinaryTable::getLongP1(const uint8_t nbytes) {
    const long n = Utils::decode_longFixedBytes(b_P1, nbytes);
    b_P1 += nbytes;
    return n;
}

int BinaryTable::getByte() {
    currentPos++;
    return b[b_current++];
}

short BinaryTable::getCurrentFile() {
    return currentFile;
}

int BinaryTable::getCurrentPosition() {
    return currentPos;
}

short BinaryTable::getLimitFile() {
    return limitFile;
}

int BinaryTable::getLimitPosition() {
    return limitPos;
}

void BinaryTable::mark() {
    markFile = currentFile;
    markPos = currentPos;
    markValue1 = currentValue1;
    markValue2 = currentValue2;
    markHasNextFlag = hasNextFlag;
    markHasNextChecked = hasNextChecked;
    markNextDone = nextDone;
    markNextValue1 = nextValue1;
    markNextValue2 = nextValue2;
}

void BinaryTable::reset(const char i) {
    currentFile = markFile;
    currentPos = markPos;
    currentValue1 = markValue1;
    currentValue2 = markValue2;
    hasNextFlag = markHasNextFlag;
    hasNextChecked = markHasNextChecked;
    nextDone = markNextDone;
    nextValue1 = markNextValue1;
    nextValue2 = markNextValue2;
    setPosition(currentFile, currentPos);
}

void BinaryTable::setNextFromConstraints() {
    hasNextChecked = true;
    if (constraint1 != -1) {
        if (constraint1 != currentValue1) {
            hasNextFlag = false;
            return;
        }
        if (constraint2 != -1 && constraint2 != currentValue2) {
            hasNextFlag = false;
            return;
        }
    }
    nextValue1 = currentValue1;
    nextValue2 = currentValue2;
    hasNextFlag = true;
    nextDone = true;
}
