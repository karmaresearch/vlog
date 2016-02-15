#include <trident/binarytables/newcolumntable.h>

void NewColumnTable::next_pair() {
    if (!has_next()) {
        return;
    }

    //Read first term
    if (isSecondColumnIgnored() || scannedCounts == currentCount) {
        //setPosition(startpos1 + bytesFirstBlock * currentFirstIdx);
        currentValue1 = getLongP1(bytesPerFirstEntry);
        currentCount = getLongP1(bytesPerCount);
        skipP1(bytesPerStartingPoint);

        currentFirstIdx++;
        scannedCounts = 0;
        firstIndexSecondColumn = currentIdx;
    }

    if (!isSecondColumnIgnored()) {
        //Read second term
        setPosition(startpos2 + bytesPerSecondEntry * currentIdx);
        currentValue2 = getLong(bytesPerSecondEntry);
        scannedCounts++;
        currentIdx++;
    }

    if (isSecondColumnIgnored() && currentFirstIdx == nUniqueFirstTerms) {
        setPosition(getLimitFile(), getLimitPosition());
    }
}

void NewColumnTable::first() {
    advance();
    uint8_t header1 = (uint8_t) getByte();
    bytesPerFirstEntry = (header1 >> 3) & 7;
    bytesPerSecondEntry = (header1) & 7;
    uint8_t header2 = (uint8_t) getByte();
    bytesPerStartingPoint =  header2 & 7;
    bytesPerCount = (header2 >> 3) & 7;
    bytesFirstBlock = bytesPerFirstEntry + bytesPerCount + bytesPerStartingPoint;

    nUniqueFirstTerms = getVLong2();
    nTerms = getVLong2();

    startpos1 = getCurrentPosition();
    setP1();

    startpos2 = startpos1 + (bytesFirstBlock) * nUniqueFirstTerms;
    currentFirstIdx = currentIdx = firstIndexSecondColumn = 0;
    scannedCounts = currentCount = 0;

    assert(bytesPerFirstEntry > 0);
    assert(bytesPerSecondEntry > 0);

    next_pair();
}

void NewColumnTable::moveToClosestFirstTerm(long c1) {
    assert(bytesFirstBlock > 0);
    assert(bytesPerSecondEntry > 0);
    if (c1 < currentValue1)
        return;

    if (c1 == currentValue1) {
        //Move to the first second value of the group
        currentIdx = firstIndexSecondColumn;
        scannedCounts = 0;
    } else {
        //Binary search
        uint64_t start = startpos1 + bytesFirstBlock * currentFirstIdx;
        uint64_t end = startpos2;
        bool found = false;
        while (start < end) {
            const uint64_t middlePos = (end - start) / bytesFirstBlock / 2;
            uint64_t middle = start + middlePos * bytesFirstBlock;
            setPosition(middle);
            const uint64_t middleValue = getLong(bytesPerFirstEntry);
            if (middleValue < c1) {
                start = middle + bytesFirstBlock;
            } else if (middleValue > c1) {
                end = middle;
            } else { //Found
                currentCount = getLong(bytesPerCount);
                currentIdx = firstIndexSecondColumn = getLong(bytesPerStartingPoint);
                currentFirstIdx = currentFirstIdx + middlePos + 1;
                currentValue1 = middleValue;
                scannedCounts = 0;
                found = true;
                setP1();
                break;
            }
        }

        if (!found) {
            if (start >= startpos2) {
                start = startpos2 - bytesFirstBlock;
                setPosition(start);
                currentValue1 = getLong(bytesPerFirstEntry);
                currentCount = getLong(bytesPerCount);
                currentIdx = firstIndexSecondColumn = getLong(bytesPerStartingPoint);
                currentFirstIdx = (start - startpos1) / bytesFirstBlock;
                scannedCounts = 0;
            }
        }
    }
    scannedCounts = 0;

    if (!isSecondColumnIgnored()) { //Read the second value
        setPosition(startpos2 + bytesPerSecondEntry * currentIdx);
        currentValue2 = getLong(bytesPerSecondEntry);
        scannedCounts++;
        currentIdx++;
    }
}

void NewColumnTable::moveToClosestSecondTerm(long c1, long c2) {
    if (currentValue1 > c1) {
        return;
    } else if (currentValue1 < c1) {
        moveToClosestFirstTerm(c1);
    }

    if (c1 == currentValue1) { //Binary search
        uint64_t start = startpos2 + bytesPerSecondEntry * currentIdx;
        uint64_t initialStart = start;
        uint64_t end = startpos2 + bytesPerSecondEntry * nTerms;
        bool found = false;
        while (start < end) {
            const uint64_t middlePos = (end - start) / bytesPerSecondEntry / 2;
            uint64_t middle = start + middlePos * bytesPerSecondEntry;
            setPosition(middle);
            const uint64_t middleValue = getLong(bytesPerSecondEntry);
            if (middleValue < c2) {
                start = middle + bytesPerSecondEntry;
            } else if (middleValue > c2) {
                end = middle;
            } else { //Found
                found = true;
                currentValue2 = middleValue;
                scannedCounts = scannedCounts + middlePos + 1;
                currentIdx = currentIdx + middlePos + 1;
                break;
            }
        }
        if (!found) {
            if (start >= (startpos2 + bytesPerSecondEntry * nTerms)) {
                start = start - bytesPerSecondEntry;
            }
            setPosition(start);
            currentValue2 = getLong(bytesPerSecondEntry);
            scannedCounts = (start - startpos2) / bytesPerSecondEntry + 1;
            currentIdx += (start - initialStart) / bytesPerSecondEntry + 1;
        }
    }
}

void NewColumnTable::mark() {
    BinaryTable::mark();
    _m_currentIdx = currentIdx;
    _m_currentFirstIdx = currentFirstIdx;
    _m_scannedCounts = scannedCounts;
    _m_currentCount = currentCount;
}

void NewColumnTable::reset(const char i) {
    currentIdx = _m_currentIdx;
    currentFirstIdx = _m_currentFirstIdx;
    scannedCounts = _m_scannedCounts;
    currentCount = _m_currentCount;
    BinaryTable::reset(i);
}

uint64_t NewColumnTable::getNFirsts() {
    return nUniqueFirstTerms;
}

uint64_t NewColumnTable::estNFirsts() {
    return nUniqueFirstTerms;
}

uint64_t NewColumnTable::getNSecondsFixedFirst() {
    return getCount();
}

uint64_t NewColumnTable::estNSecondsFixedFirst() {
    return getCount();
}

void NewColumnTable::columnNotIn(uint8_t columnId, BinaryTable *other,
                                 uint8_t columnOther, SequenceWriter *output) {
    if (columnId == 1 && columnOther == 1) {
        columnNotIn11(getBufferAtPos(startpos1),
                      getBufferAtPos(startpos2),
                      bytesPerFirstEntry, bytesFirstBlock,
                      other->getBufferAtPos(((NewColumnTable*)other)->startpos1),
                      other->getBufferAtPos(((NewColumnTable*)other)->startpos2),
                      ((NewColumnTable*)other)->bytesPerFirstEntry,
                      ((NewColumnTable*)other)->bytesFirstBlock,
                      output);
    } else if (columnId == 1 && columnOther == 2) {
        uint8_t size2 = ((NewColumnTable*)other)->bytesPerSecondEntry;
        char *start2 = other->getBufferAtPos(
                           ((NewColumnTable*)other)->startpos2);
        start2 = start2 + ((NewColumnTable*)other)->firstIndexSecondColumn *
                 size2;
        char *end2 = start2 + size2 * other->getCount();
        columnNotIn12(getBufferAtPos(startpos1),
                      getBufferAtPos(startpos2),
                      bytesPerFirstEntry, bytesFirstBlock,
                      start2, end2, size2, output);
    } else if (columnId == 2 && columnOther == 1) {
        uint8_t size1 = bytesPerSecondEntry;
        char *start1 = getBufferAtPos(startpos2) +
                       firstIndexSecondColumn * size1;
        char *end1 = start1 + size1 * getCount();
        columnNotIn21(start1, end1, size1,
                      other->getBufferAtPos(((NewColumnTable*)other)->startpos1),
                      other->getBufferAtPos(((NewColumnTable*)other)->startpos2),
                      ((NewColumnTable*)other)->bytesPerFirstEntry,
                      ((NewColumnTable*)other)->bytesFirstBlock,
                      output);

    } else if (columnId == 2 && columnOther == 2) {
        uint8_t size1 = bytesPerSecondEntry;
        char *start1 = getBufferAtPos(startpos2) +
                       firstIndexSecondColumn * size1;
        char *end1 = start1 + size1 * getCount();
        uint8_t size2 = ((NewColumnTable*)other)->bytesPerSecondEntry;
        char *start2 = other->getBufferAtPos(
                           ((NewColumnTable*)other)->startpos2);
        start2 = start2 + ((NewColumnTable*)other)->firstIndexSecondColumn *
                 size2;
        char *end2 = start2 + size2 * other->getCount();
        columnNotIn22(start1, end1, size1, start2, end2, size2, output);
    } else {
        throw 10;
    }
}

long NewColumnTable::getCount() {
    return currentCount;
}

void NewColumnTable::ignoreSecondColumn() {
    BinaryTable::ignoreSecondColumn();
    if (isSecondColumnIgnored() && currentFirstIdx == nUniqueFirstTerms) {
        setPosition(getLimitFile(), getLimitPosition());
    }
}

void NewColumnTable::columnNotIn11(char *begin1, char* end1,
                                   const uint8_t bEntry1, const uint8_t bBlock1,
                                   char *begin2, char *end2,
                                   const uint8_t bEntry2, const uint8_t bBlock2,
                                   SequenceWriter *output) {

    uint64_t tv, ov;
    tv = Utils::decode_longFixedBytes(begin1, bEntry1);
    ov = Utils::decode_longFixedBytes(begin2, bEntry2);
    do {
        if (tv <= ov) {
            if (tv < ov)
                output->add(tv);
            //move tv forward
            begin1 += bBlock1;
            if (begin1 != end1) {
                tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            } else {
                break;
            }
        } else {
            begin2 += bBlock2;
            if (begin2 != end2) {
                ov = Utils::decode_longFixedBytes(begin2, bEntry2);
            } else {
                break;
            }
        }
    } while (true);

    if (begin1 != end1) {
        output->add(tv);
        begin1 += bBlock1;
        while (begin1 != end1) {
            tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            output->add(tv);
            begin1 += bBlock1;
        }
    }
}

void NewColumnTable::columnNotIn12(char *begin1, char* end1,
                                   const uint8_t bEntry1, const uint8_t bBlock1,
                                   char *begin2, char *end2,
                                   const uint8_t bEntry2,
                                   SequenceWriter * output) {
    uint64_t tv, ov;
    tv = Utils::decode_longFixedBytes(begin1, bEntry1);
    ov = Utils::decode_longFixedBytes(begin2, bEntry2);
    do {
        if (tv <= ov) {
            if (tv < ov)
                output->add(tv);
            //move tv forward
            begin1 += bBlock1;
            if (begin1 != end1) {
                tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            } else {
                break;
            }
        } else {
            begin2 += bEntry2;
            if (begin2 != end2) {
                ov = Utils::decode_longFixedBytes(begin2, bEntry2);
            } else {
                break;
            }
        }
    } while (true);

    if (begin1 != end1) {
        output->add(tv);
        begin1 += bBlock1;
        while (begin1 != end1) {
            tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            output->add(tv);
            begin1 += bBlock1;
        }
    }
}

void NewColumnTable::columnNotIn21(char *begin1, char* end1,
                                   const uint8_t bEntry1,
                                   char *begin2, char *end2,
                                   const uint8_t bEntry2, const uint8_t bBlock2,
                                   SequenceWriter * output) {
    uint64_t tv, ov;
    tv = Utils::decode_longFixedBytes(begin1, bEntry1);
    ov = Utils::decode_longFixedBytes(begin2, bEntry2);
    do {
        if (tv <= ov) {
            if (tv < ov)
                output->add(tv);
            //move tv forward
            begin1 += bEntry1;
            if (begin1 != end1) {
                tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            } else {
                break;
            }
        } else {
            begin2 += bBlock2;
            if (begin2 != end2) {
                ov = Utils::decode_longFixedBytes(begin2, bEntry2);
            } else {
                break;
            }
        }
    } while (true);

    if (begin1 != end1) {
        output->add(tv);
        begin1 += bEntry1;
        while (begin1 != end1) {
            tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            output->add(tv);
            begin1 += bEntry1;
        }
    }
}

void NewColumnTable::columnNotIn22(char *begin1, char* end1,
                                   const uint8_t bEntry1,
                                   char *begin2, char *end2,
                                   const uint8_t bEntry2,
                                   SequenceWriter * output) {
    uint64_t tv, ov;
    tv = Utils::decode_longFixedBytes(begin1, bEntry1);
    ov = Utils::decode_longFixedBytes(begin2, bEntry2);
    do {
        if (tv <= ov) {
            if (tv < ov)
                output->add(tv);
            //move tv forward
            begin1 += bEntry1;
            if (begin1 != end1) {
                tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            } else {
                break;
            }
        } else {
            begin2 += bEntry2;
            if (begin2 != end2) {
                ov = Utils::decode_longFixedBytes(begin2, bEntry2);
            } else {
                break;
            }
        }
    } while (true);

    if (begin1 != end1) {
        output->add(tv);
        begin1 += bEntry1;
        while (begin1 != end1) {
            tv = Utils::decode_longFixedBytes(begin1, bEntry1);
            output->add(tv);
            begin1 += bEntry1;
        }
    }
}
