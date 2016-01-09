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

#ifndef PAIRHANDLER_H_
#define PAIRHANDLER_H_

#include <trident/storage/fileindex.h>
#include <trident/files/filemanager.h>
#include <trident/files/comprfiledescriptor.h>

#include <tridentcompr/utils/utils.h>

#define BUFFER_SIZE 1048576

#define ADDITIONAL_SECOND_INDEX_SIZE 256
#define FIRST_INDEX_SIZE 128
#define MIN_SIZE_BINARYSEARCH2 64

class SequenceWriter {
    public:
        virtual void add(const uint64_t v) {
            throw 10; //The default class should never be used
        }
};

class PairHandler {
private:
    long markValue1, markValue2;
    int markPos;

    int basePos;
    char* b;
    int b_start, b_current, b_length;
    short markFile;
    short baseFile;
    FileManager<ComprFileDescriptor, FileSegment> *manager;
    int sessionId;

protected:
    FileIndex *index;

    long currentValue1, currentValue2;
    int currentPos, limitPos;
    short currentFile, limitFile;

    void advance();
    long getVLong();
    long getVLong2();
    long getLong();
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

    int get_current_file_size() {
        return manager->sizeFile(currentFile);
    }

    int getRelativePosition() {
        if (getCurrentFile() != baseFile) {
            return getCurrentPosition();
        } else {
            return getCurrentPosition() - basePos;
        }
    }

    int getLimitPosition();

    short getLimitFile();

    void setPosition(const int pos);

    void setEndChunk();

    size_t getFileSize(const short idFile) {
        return manager->sizeFile(idFile);
    }
    long getNBytesFrom(short file, int pos);

    void setRelativePosition(short file, int pos);

    //Insert methods
    int writeLong(long t) {
        manager->appendLong(t);
        currentPos += 8;
        return 8;
    }

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

public:
    PairHandler();

    void init(FileManager<ComprFileDescriptor, FileSegment> *manager,
              FileIndex * idx);

    bool more_data();

    void setBasePosition(short file, int pos);

    void setLimitPosition(short file, int pos);

    void setPosition(const short file, const int pos);

    long value1();

    long value2();

    int getCurrentPosition();

    short getCurrentFile();

    virtual void mark();

    virtual void reset();

    virtual int getType() = 0;

    virtual uint64_t estimateNPairs() = 0;

    virtual void next_pair() {
    };

    virtual void columnNotIn(uint8_t columnId, PairHandler *other,
                             uint8_t columnOther, SequenceWriter *output) = 0;

    virtual void moveToClosestFirstTerm(long c1) = 0;

    virtual void moveToClosestSecondTerm(long c1, long c2) = 0;

    virtual void start_reading() {
    };

    virtual void ignoreSecondColumn() = 0;

    void clear();

    virtual ~PairHandler() {
	clear();
    };

    void cleanup();

    void setup(FileManager<ComprFileDescriptor, FileSegment> *cache, short file,
               int pos, FileIndex *index);

    /***Used for inserting***/
    virtual void startAppend() = 0;

    virtual void append(long t1, long t2) = 0;

    virtual void stopAppend() = 0;

    void appendPair(const long t1, const long t2);
};

class GroupPairHandler: public PairHandler {
private:
    //Common vars
    long previousFirstTerm, previousSecondTerm;
    short baseSecondTermFile;
    int baseSecondTermPos;
    short fileLastFirstTerm;
    int posLastFirstTerm;

    FileIndex *secondTermIndex;
    bool removeSecondTermIndex;

    //Vars used during the reading
    bool shouldReadFirstTerm;
    int readBytes;
    int remainingBytes;
    short nextFileMark;
    int nextPosMark;
    int currentSecondTermIdx;

    // When I update the collection, I need to know the position of the last
    // read term
    int positionSecondTerm;

    int diffMode1;
    int compr1;
    int compr2;

    //Used for marking
    long mPreviousFirstTerm, mPreviousSecondTerm;
    short mBaseSecondTermFile;
    int mBaseSecondTermPos;
    short mFileLastFirstTerm;
    int mPosLastFirstTerm;
    FileIndex *mSecondTermIndex;
    bool mShouldReadFirstTerm;
    int mReadBytes;
    int mRemainingBytes;
    short mNextFileMark;
    int mNextPosMark;
    int mCurrentSecondTermIdx;
    int mPositionSecondTerm;

    //Private functions
    long readFirstTerm();
    long readSecondTerm();
    void setDataStructuresBeforeNewSecondValue(const int flag);
    int getAbsoluteSecondTermPos(short file, int relPos);
    bool checkSecondGroupIsFinished();

    //*** INSERT ***/
    long lastSecondTerm;
    long nElementsForIndexing;

    //Additional vars used to write the second term
    int bytesUsed;
    bool smallGroupMode;
    int nElementsGroup;

    int getRelativeSecondTermPos();
    int getRelativeSecondTermPos(short file, int absPos);
    void writeNElementsAt(char b, short file, int pos);
    void insertSecondTerm(bool last);
    void updateSecondTermIndex(long lastTermWritten, int bytesTaken,
                               short currentFile, int currentPos);
    long calculateSecondTermToWrite(long term);
    int writeSecondTerm(long termToWrite);
    void updateFirstTermIndex(const long t1);
    void writeFirstTerm(long termToWrite);
    long calculateFirstTermToWrite(long termToWrite);

public:
    GroupPairHandler();

    void next_pair();
    void start_reading();
    void mode_difference(int modeValue1);
    void mode_compression(int compr1, int compr2);
    void moveToClosestFirstTerm(long c1);
    void moveToClosestSecondTerm(long c1, long c2);
    void mark();
    void reset();
    uint64_t estimateNPairs();
    static int search_value(const char *b, int start, int end, const long v);
    int search_value_with_check(int start, int end, const long v);


    int getType() {
        return CLUSTER_LAYOUT;
    }

    void ignoreSecondColumn() {
        throw 10;
        //not yet supported
    }

    void columnNotIn(uint8_t columnId, PairHandler *other,
                     uint8_t columnOther, SequenceWriter *output) {
        throw 10;
        //not yet supported
    }

    void startAppend();
    void append(long t1, long t2);
    void stopAppend();

    ~GroupPairHandler() {
        if (removeSecondTermIndex && secondTermIndex != NULL) {
            delete secondTermIndex;
        }
    }
};

class ListPairHandler: public PairHandler {
private:
    int comprValue1;
    int comprValue2;
    int diffValue1;

    long previousValue1, markPreviousValue1;

    long readFirstTerm();
    long readSecondTerm();

    //Used for inserting
    long nElements;

    void writeFirstTerm(long t1);
    void writeSecondTerm(long t2);

public:
    void next_pair();
    void start_reading();
    void moveToClosestFirstTerm(long c1);
    void moveToClosestSecondTerm(long c1, long c2);
    void mark();
    void reset();
    void setCompressionMode(int v1, int v2);
    void setDifferenceMode(int d1);
    uint64_t estimateNPairs();

    int getType() {
        return ROW_LAYOUT;
    }

    void ignoreSecondColumn() {
        throw 10;
        //not yet supported
    }

    void columnNotIn(uint8_t columnId, PairHandler *other,
                     uint8_t columnOther, SequenceWriter *output) {
        throw 10;
        //not yet supported
    }

    void startAppend();
    void append(long t1, long t2);
    void stopAppend();
};

class SimplifiedGroupPairHandler: public PairHandler {
private:

    int compr1;
    int compr2;
    uint8_t bytesPerFirstEntry, bytesPerPointer;

    uint64_t firstValue1, firstValue2;

    short startfile1, endfile1, startfile2, endfile2;
    int startpos1, endpos1, startpos2, endpos2;

//   short baseSecondTermFile;
//   int baseSecondTermPos;

    uint64_t idxNextPointer, nFirstEntries;
    FileIndex *secondTermIndex;
    size_t lastSecondTermIndex;
    size_t lastSecondTermIndexPos;

    //We want to read only the first column
    bool onlyFirstValues;

    //Used for marking
    uint64_t m_idxNextPointer;
    short m_startfile2, m_endfile2;
    int m_startpos2, m_endpos2;
    uint64_t m_firstValue2;
    FileIndex *mSecondTermIndex;

    size_t m_lastSecondTermIndex;
    size_t m_lastSecondTermIndexPos;

    //Used during insert
    uint64_t largestElement;
    std::vector<std::pair<uint64_t, uint64_t>> tmpfirstpairs;
    std::vector<uint64_t> tmpsecondpairs;

    long readFirstTerm();

    long readSecondTerm();

    void writeFirstTerm(long t1);

    void writeSecondTerm(long t2);

    uint8_t getNBytes(const int comprType, const long value) const;

    void setGroup(const short file, const size_t pos, const size_t blockSize);

    void columnNotIn11(SimplifiedGroupPairHandler *p1,
                       SimplifiedGroupPairHandler *p2,
                       SequenceWriter *output);
public:
    void next_pair();

    void start_reading();

    void moveToClosestFirstTerm(long c1);

    void moveToClosestSecondTerm(long c1, long c2);

    void mark();

    void reset();

    void ignoreSecondColumn();

    void setCompressionMode(int v1, int v2);

    uint64_t estimateNPairs();

    uint64_t getNFirstColumn();

    int getType() {
        return COLUMN_LAYOUT;
    }

    void columnNotIn(uint8_t columnId, PairHandler *other,
                     uint8_t columnOther, SequenceWriter *output);

    void appendBlock();

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();
};

#endif /* PAIRHANDLER_H_ */
