#ifndef _CLUSTERTABLE_H
#define _CLUSTERTABLE_H

#include <trident/binarytables/binarytable.h>

class ClusterTable: public BinaryTable {
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


public:
    ClusterTable();

    void next_pair();

    void first();

    void mode_difference(int modeValue1);

    void mode_compression(int compr1, int compr2);

    void moveToClosestFirstTerm(long c1);

    void moveToClosestSecondTerm(long c1, long c2);

    long getCount();

    uint64_t getNFirsts();

    uint64_t estNFirsts();

    uint64_t getNSecondsFixedFirst();

    uint64_t estNSecondsFixedFirst();

    static int search_value(const char *b, int start, int end, const long v);

    int search_value_with_check(int start, int end, const long v);

    int getType() {
        return CLUSTER_ITR;
    }

    void columnNotIn(uint8_t columnId, BinaryTable *other,
                     uint8_t columnOther, SequenceWriter *output) {
        throw 10; //not supported
    }

    ~ClusterTable() {
        if (removeSecondTermIndex && secondTermIndex != NULL) {
            delete secondTermIndex;
        }
    }

    void mark();

    void reset(const char i);

};

#endif
