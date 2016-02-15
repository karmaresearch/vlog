#ifndef PAIRHANDLER_H_
#define PAIRHANDLER_H_

#include <trident/iterators/pairitr.h>
#include <trident/binarytables/fileindex.h>
#include <trident/files/filemanager.h>
#include <trident/files/comprfiledescriptor.h>
#include <trident/files/filedescriptor.h>

#include <tridentcompr/utils/utils.h>

#define BUFFER_SIZE 16*1024*1024
#define MIN_SIZE_BINARYSEARCH2 64

class SequenceWriter {
public:
    virtual void add(const uint64_t v) {
        throw 10; //The default class should never be used
    }
};

class BinaryTable : public PairItr {
private:
    long markValue1, markValue2;
    long markNextValue1, markNextValue2;
    int markPos;
    short markFile;
    bool markHasNextFlag, markHasNextChecked, markNextDone;

    int basePos;
    int currentPos, limitPos;
    short currentFile, limitFile;
    short baseFile;
    bool secondColumnIgnored;

    char* b;
    int b_start, b_current, b_length;
    FileManager<FileDescriptor, FileDescriptor> *manager;
    int sessionId;

    bool hasNextFlag, hasNextChecked, nextDone;

    char* b_P1;
    char* b_P2;

protected:
    FileIndex *index;

    long currentValue1, currentValue2;
    long nextValue1, nextValue2;

    void advance();

    long getVLong();

    long getVLong2();

    long getLong();

    long getLong(const uint8_t nbytes);

    long getLongP1(const uint8_t nbytes);

    int getByte();

    int getAbsPosition(int file, int relPos) {
        return (file != baseFile) ? relPos : relPos + basePos;
    }

    //These three methods are used only during the comprPairHandler::search
    char* buffer() {
        return b;
    }
    int b_start_pos() {
        return b_start;
    }
    int b_len() {
        return b_length;
    }

    int getLimitPosition();

    short getLimitFile();

    void setPosition(const int pos);

    void setP1();

    void skipP1(const uint8_t nbytes) {
        b_P1 += nbytes;
    }

    void setEndChunk();

    size_t getFileSize(const short idFile) {
        return manager->sizeFile(idFile);
    }

    size_t getCurrentFileSize() {
        return manager->sizeFile(currentFile);
    }

    void setRelativePosition(short file, int pos);

public:
    BinaryTable();

    char *getBufferAtPos(const int pos) {
        return b + pos - b_start;
    }

    void init(FileManager<FileDescriptor, FileDescriptor> *manager,
              FileIndex * idx);

    void setBasePosition(short file, int pos);

    void setLimitPosition(short file, int pos);

    void setPosition(const short file, const int pos);

    bool has_next();

    int getCurrentPosition();

    short getCurrentFile();

    void clear();

    virtual ~BinaryTable() {
        clear();
    };

    virtual int getType() = 0;

    virtual uint64_t getNFirsts() = 0;

    virtual uint64_t estNFirsts() = 0;

    virtual uint64_t getNSecondsFixedFirst() = 0;

    virtual uint64_t estNSecondsFixedFirst() = 0;

    virtual void next_pair() = 0;

    virtual void columnNotIn(uint8_t columnId, BinaryTable *other,
                             uint8_t columnOther, SequenceWriter *output) = 0;

    virtual void ignoreSecondColumn() {
        secondColumnIgnored = true;
    }

    virtual void considerSecondColumn() {
        secondColumnIgnored = false;
    }

    bool isSecondColumnIgnored() const {
        return secondColumnIgnored;
    }

    void setNextFromConstraints();

    virtual void first() = 0;

    virtual void moveToClosestFirstTerm(long c1) = 0;

    virtual void moveToClosestSecondTerm(long c1, long c2) = 0;

    long getValue1();

    long getValue2();

    void next() {
        if (hasNextChecked) {
            if (hasNextFlag) {
                if (nextDone) {
                    currentValue1 = nextValue1;
                    currentValue2 = nextValue2;
                } else {
                    next_pair();
                }
                hasNextChecked = false;
            }
        } else {
            next_pair();
        }
    }

    bool hasNext() {
        if (hasNextChecked)
            return hasNextFlag;

        if (has_next()) {
            if (constraint1 != -1 || secondColumnIgnored) {
                const long o1 = currentValue1;
                const long o2 = currentValue2;
                next_pair();
                if (secondColumnIgnored) {
                    if (getValue1() == o1) {
                        hasNextFlag = false;
                        return hasNextFlag;
                    }
                } else {
                    if (getValue1() != constraint1) {
                        hasNextFlag = false;
                        return hasNextFlag;
                    }
                    if (constraint2 != -1) {
                        if (getValue2() != constraint2) {
                            hasNextFlag = false;
                            return hasNextFlag;
                        }
                    }
                }
                nextValue1 = currentValue1;
                nextValue2 = currentValue2;
                currentValue1 = o1;
                currentValue2 = o2;
                nextDone = true;
            } else {
                nextDone = false;
            }
            hasNextFlag = true;
        } else {
            hasNextFlag = false;
        }
        hasNextChecked = true;
        return hasNextFlag;
    }

    uint64_t getCardinality() {
        if (isSecondColumnIgnored()) {
            return getNFirsts();
        } else {
            assert(constraint1 != -1);
            return getNSecondsFixedFirst();
        }
    }

    uint64_t estCardinality() {
        if (isSecondColumnIgnored()) {
            return estNFirsts();
        } else {
            assert(constraint1 != -1);
            return estNSecondsFixedFirst();
        }
    }

    int getTypeItr() {
        return getType();
    }

    void gotoFirstTerm(long c1) {
        if (!nextDone || c1 > nextValue1)
            moveToClosestFirstTerm(c1);
        if (getValue1() < c1) {
            hasNextChecked = true;
            hasNextFlag = false;
        } else {
            setNextFromConstraints();
        }
    }

    void gotoSecondTerm(long c2) {
        if (!nextDone || c2 > nextValue2)
            moveToClosestSecondTerm(getValue1(), c2);
        if (getValue2() < c2) {
            hasNextChecked = true;
            hasNextFlag = false;
        } else {
            setNextFromConstraints();
        }
    }

    virtual void mark();

    virtual void reset(const char i);
};

#endif
