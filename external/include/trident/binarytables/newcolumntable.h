#ifndef _NEWCOLUMNTABLE_H
#define _NEWCOLUMNTABLE_H

#include <trident/binarytables/binarytable.h>

class NewColumnTable: public BinaryTable {
private:
    uint8_t bytesPerFirstEntry, bytesPerSecondEntry;
    uint8_t bytesPerCount, bytesPerStartingPoint;
    uint8_t bytesFirstBlock;

    uint64_t nTerms;
    uint64_t nUniqueFirstTerms;
    uint64_t startpos1, startpos2;

    uint64_t currentFirstIdx, currentIdx, scannedCounts, currentCount;
    uint64_t firstIndexSecondColumn;

    uint64_t _m_currentFirstIdx, _m_currentIdx, _m_scannedCounts,
             _m_currentCount;
    uint64_t _m_firstIndexSecondColumn;

    static void columnNotIn11(char *begin1, char* end1,
                              const uint8_t bEntry1, const uint8_t bBlock1,
                              char *begin2, char *end2,
                              const uint8_t bEntry2, const uint8_t bBlock2,
                              SequenceWriter *output);

    static void columnNotIn12(char *begin1, char* end1,
                              const uint8_t bEntry1, const uint8_t bBlock1,
                              char *begin2, char *end2,
                              const uint8_t bEntry2,
                              SequenceWriter *output);

    static void columnNotIn21(char *begin1, char* end1,
                              const uint8_t bEntry1,
                              char *begin2, char *end2,
                              const uint8_t bEntry2, const uint8_t bBlock2,
                              SequenceWriter *output);

    static void columnNotIn22(char *begin1, char* end1,
                              const uint8_t bEntry1,
                              char *begin2, char *end2,
                              const uint8_t bEntry2,
                              SequenceWriter *output);
public:
    void next_pair();

    void first();

    void moveToClosestFirstTerm(long c1);

    void moveToClosestSecondTerm(long c1, long c2);

    void mark();

    void reset(const char i);

    uint64_t getNFirsts();

    uint64_t estNFirsts();

    long getCount();

    uint64_t getNSecondsFixedFirst();

    uint64_t estNSecondsFixedFirst();

    void ignoreSecondColumn();

    int getType() {
        return NEWCOLUMN_ITR;
    }

    void columnNotIn(uint8_t columnId, BinaryTable *other,
                     uint8_t columnOther, SequenceWriter *output);

};

#endif
