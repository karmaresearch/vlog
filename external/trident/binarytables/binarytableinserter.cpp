#include <trident/binarytables/binarytableinserter.h>

int BinaryTableInserter::writeVLong2(long t) {
    int prevPos = currentPos;
    currentPos = manager->appendVLong2(t);
    return currentPos - prevPos;
}

void BinaryTableInserter::writeLong(const uint8_t nbytes, const long v) {
    manager->appendLong(nbytes, v);
    currentPos += nbytes;
}

void BinaryTableInserter::overwriteVLong2(short file, int pos, long number) {
    manager->overwriteVLong2At(file, pos, number);
}

//reserve "bytes" consecutive bytes
void BinaryTableInserter::reserveBytes(const uint8_t bytes) {
    manager->reserveBytes(bytes);
    currentPos += bytes;
}

void BinaryTableInserter::createNewFileIfCurrentIsTooLarge() {
    if (manager->sizeLastFile() + 17 >= manager->getFileMaxSize()) {
        currentFile = manager->createNewFile();
        currentPos = 0;
    }
}

long BinaryTableInserter::getNBytesFrom(short file, int pos) {
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

void BinaryTableInserter::appendPair(const long t1, const long t2) {
    //Check if there is enough space
    if (currentPos + 17 > manager->getFileMaxSize()) {
        currentFile = manager->createNewFile();
        currentPos = 0;
    }
    append(t1, t2);
}

void BinaryTableInserter::setup(FileManager<FileDescriptor, FileDescriptor> *manager,
                                /*short file,
                                int p,*/ FileIndex *index) {
    this->manager = manager;
    this->index = index;
    //setPosition(file, p);
}

void BinaryTableInserter::cleanup() {
}

void BinaryTableInserter::setBasePosition(short file, int pos) {
    this->baseFile = file;
    this->basePos = pos;
    this->currentFile = file;
    this->currentPos = pos;
}

int BinaryTableInserter::getCurrentPosition() {
    return currentPos;
}

short BinaryTableInserter::getCurrentFile() {
    return currentFile;
}
