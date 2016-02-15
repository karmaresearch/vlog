#ifndef _BINARYTABLEINSERTER_H
#define _BINARYTABLEINSERTER_H

#include <trident/files/filemanager.h>
#include <trident/files/comprfiledescriptor.h>
#include <trident/files/filedescriptor.h>

#include <trident/binarytables/fileindex.h>

class BinaryTableInserter {
private:
    FileManager<FileDescriptor, FileDescriptor> *manager;
    short currentFile;
    int currentPos;
    int basePos;
    short baseFile;

protected:
    FileIndex *index;

    int writeLong(long t) {
        manager->appendLong(t);
        currentPos += 8;
        return 8;
    }

    void writeLong(const uint8_t nbytes, const long v);

    int writeVLong(long t) {
        int prevPos = currentPos;
        currentPos = manager->appendVLong(t);
        return currentPos - prevPos;
    }

    int writeVLong2(long t);

    void overwriteVLong2(short file, int pos, long number);

    int writeByte(char t) {
        manager->append(&t, 1);
        currentPos++;
        return 1;
    }

    void overwriteBAt(char b, short file, int pos) {
        manager->overwriteAt(file, pos, b);
    }

    void reserveBytes(const uint8_t bytes);

    void createNewFileIfCurrentIsTooLarge();

    int getRelativePosition() {
        if (getCurrentFile() != baseFile) {
            return getCurrentPosition();
        } else {
            return getCurrentPosition() - basePos;
        }
    }

    long getNBytesFrom(short file, int pos);

    size_t getFileSize(const short idFile) {
        assert(idFile <= manager->getIdLastFile());
        return manager->sizeFile(idFile);
    }

    void setPosition(const int pos);

    int getBasePosition() {
        return basePos;
    }

    //void setPosition(const short file, const int pos);

public:
    void setup(FileManager<FileDescriptor, FileDescriptor> *cache,
               FileIndex *index);

    void setBasePosition(short file, int pos);

    virtual void startAppend() = 0;

    virtual void append(long t1,
                        long t2) = 0;

    virtual void stopAppend() = 0;

    virtual int getType() = 0;

    void appendPair(const long t1,
                    const long t2);

    int getCurrentPosition();

    short getCurrentFile();

    void cleanup();

    virtual ~BinaryTableInserter() {
    }
};
#endif
