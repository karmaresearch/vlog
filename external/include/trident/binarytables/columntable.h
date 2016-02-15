#ifndef _COLUMNTABLE_H
#define _COLUMNTABLE_H

#include <trident/binarytables/binarytable.h>

class ColumnTable: public BinaryTable {
private:

    int compr1;
    int compr2;
    uint8_t bytesPerFirstEntry, bytesPerPointer, bytesPerNElements;

    uint64_t firstValue1, firstValue2;
    uint64_t countFirstValue1;

    short startfile1, endfile1, startfile2, endfile2;
    int startpos1, endpos1, startpos2, endpos2;


    uint64_t idxNextPointer, nFirstEntries;
    FileIndex *secondTermIndex;
    size_t lastSecondTermIndex;
    size_t lastSecondTermIndexPos;

    //Used for marking
    uint64_t m_idxNextPointer;
    short m_startfile2, m_endfile2;
    int m_startpos2, m_endpos2;
    uint64_t m_firstValue2;
    FileIndex *mSecondTermIndex;

    size_t m_lastSecondTermIndex;
    size_t m_lastSecondTermIndexPos;

    long readFirstTerm();

    long readSecondTerm();

    void setGroup(const short file, const size_t pos, const size_t blockSize);

    void columnNotIn11(ColumnTable *p1,
                       ColumnTable *p2,
                       SequenceWriter *output);
public:
    void next_pair();

    void first();

    void moveToClosestFirstTerm(long c1);

    void moveToClosestSecondTerm(long c1, long c2);

    void mark();

    long getCount();

    void reset(const char i);

    void ignoreSecondColumn();

    void setCompressionMode(int v1, int v2);

    uint64_t getNFirsts();

    uint64_t estNFirsts();

    uint64_t getNSecondsFixedFirst();

    uint64_t estNSecondsFixedFirst();

    int getType() {
        return COLUMN_ITR;
    }

    void columnNotIn(uint8_t columnId, BinaryTable *other,
                     uint8_t columnOther, SequenceWriter *output);

};

#endif
