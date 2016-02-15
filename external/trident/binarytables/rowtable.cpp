#include <trident/binarytables/rowtable.h>

void RowTable::setCompressionMode(int v1, int v2) {
    comprValue1 = v1;
    comprValue2 = v2;
}

void RowTable::setDifferenceMode(int d1) {
    diffValue1 = d1;
}

uint64_t RowTable::getNFirsts() {
    long count = 1;
    long prevEl = currentValue1;
    while (has_next()) {
        next_pair();
        if (currentValue1 != prevEl) {
            count++;
            prevEl = currentValue1;
        }
    }
    return count;
}

uint64_t RowTable::estNFirsts() {
    //Rough estimate. Size of the table in bytes divided by 8
    int space;
    if (getCurrentFile() == getLimitFile()) {
        space = getLimitPosition() - getCurrentPosition();
    } else {
        space = getFileSize(getCurrentFile()) - getCurrentPosition();
        space += getLimitPosition();
    }
    int els = space / 8;
    if (els == 0)
        els = 1;
    return els;
}

uint64_t RowTable::getNSecondsFixedFirst() {
    long count = 1;
    long prevEl = currentValue1;
    while (has_next()) {
        next_pair();
        if (currentValue1 != prevEl) {
            break;
        } else {
            count++;
        }
    }
    return count;
}

uint64_t RowTable::estNSecondsFixedFirst() {
    return 1;
}

long RowTable::getCount() {

    //Always point to the first element. Must go to the last one with the same
    //value
    long skippedElems = 1;
    short lastFile = getCurrentFile();
    size_t lastPos = getCurrentPosition();
    while (has_next()) {
        advance();
        lastFile = getCurrentFile();
        lastPos = getCurrentPosition();
        if (currentValue1 != -1) {
            previousValue1 = currentValue1;
        }
        const long n1 = readFirstTerm();
        advance();
        readSecondTerm();
        if (n1 != currentValue1)
            break;
        skippedElems++;
    }
    setPosition(lastFile, lastPos);

    return skippedElems;
}

void RowTable::next_pair() {
    // Record the previous values and coordinates
    if (isSecondColumnIgnored()) {
        const long extCurrentValue1 = currentValue1;
        do {
            advance();
            if (currentValue1 != -1)
                previousValue1 = currentValue1;
            currentValue1 = readFirstTerm();
            // Read second element
            advance();
            currentValue2 = readSecondTerm();
        } while (has_next() && currentValue1 == extCurrentValue1);
    } else {
        advance();
        if (currentValue1 != -1)
            previousValue1 = currentValue1;
        currentValue1 = readFirstTerm();
        // Read second element
        advance();
        currentValue2 = readSecondTerm();
    }
}

void RowTable::ignoreSecondColumn() {
    BinaryTable::ignoreSecondColumn();
}

long RowTable::readFirstTerm() {
    long t = 0;
    switch (comprValue1) {
        case COMPR_1:
            t = getVLong();
            break;
        case COMPR_2:
            t = getVLong2();
            break;
        case NO_COMPR:
            t = getLong();
    }

    // Determine the value to read
    if (diffValue1 == DIFFERENCE && previousValue1 != -1) {
        t += previousValue1;
    }
    return t;
}

long RowTable::readSecondTerm() {
    switch (comprValue2) {
        case COMPR_1:
            return getVLong();
        case COMPR_2:
            return getVLong2();
        case NO_COMPR:
            return getLong();
    }
    return 0;
}

void RowTable::moveToClosestFirstTerm(long c1) {
    if (currentValue1 >= c1 || !has_next()) {
        return;
    }

    // Is there an index? Then I can move more quickly to the right
    // values...
    int posIndex = -1;
    if (index != NULL && index->sizeIndex() > 0) {
        posIndex = index->idx(c1) - 1;
        if (posIndex >= 0) {
            setRelativePosition(index->file(posIndex), index->pos(posIndex));

            // Set up the last key, in case we use store the difference
            currentValue1 = index->key(posIndex);
        }
    }

    // Read the first entry
    next_pair();

    //If the compression is of the second type and the second el is of fixed size,
    //then we can apply binary search.
    if (diffValue1 == NO_DIFFERENCE && comprValue1 == NO_COMPR
            && comprValue2 == NO_COMPR) {
        if (currentValue1 < c1 && has_next()) {
            //First calculate the end file and position
            short endFile = 0;
            int endPos = 0;
            if (index == NULL || posIndex == index->sizeIndex() - 1) {
                endPos = getLimitPosition();
                endFile = getLimitFile();
            } else {
                endFile = index->file(posIndex + 1);
                int indexPos = index->pos(posIndex + 1);
                endPos = getAbsPosition(endFile, indexPos);
            }

            /* This if contains code for a special case if the
             * interval is between two files
             */
            if (endFile != getCurrentFile()) {
                //Check the last value of the file to decide which file
                //we should search on...
                int oPos = getCurrentPosition();

                //Determine the position of the element
                int fileSize = getCurrentFileSize();
                int offset = 16 * ((fileSize - oPos) >> 4);
                bool lastPair = false;
                if ((oPos + offset) == fileSize) {
                    offset -= 16;
                    lastPair = true;
                }
                this->setPosition(oPos + offset);

                //sPos contains the starting point of the last number
                long last_num = readFirstTerm();
                if (last_num < c1) {
                    this->setPosition(endFile, lastPair ? 0 : 8);
                } else if (last_num > c1) {
                    this->setPosition(oPos);
                    endFile = getCurrentFile();
                    endPos = fileSize;
                } else {
                    currentValue1 = last_num;
                    advance();
                    currentValue2 = readSecondTerm();
                    return;
                }
            }

            //Now I now the entire range to perform a binary search...
            int beginPos = getCurrentPosition();
            long l_read_value = 0;
            while ((endPos - beginPos) >= 16) {
                int pivot = (endPos - beginPos) >> 5;
                pivot = beginPos + pivot * 16;
                setPosition(pivot);
                l_read_value = readFirstTerm();
                if (l_read_value < c1) {
                    beginPos = pivot + 16;
                } else if (l_read_value > c1) {
                    endPos = pivot;
                } else {
                    break;
                }
            }
            currentValue1 = l_read_value;
            currentValue2 = readSecondTerm();

            while (currentValue1 < c1 && has_next()) {
                next_pair();
            }
        }
    } else {
        // From now on, just continue with a linear scan
        while (currentValue1 < c1 && has_next()) {
            next_pair();
        }
    }
}

void RowTable::moveToClosestSecondTerm(long c1, long c2) {
    // Only linear search is possible...
    while (currentValue1 == c1 && currentValue2 < c2 && has_next()) {
        next_pair();
    }
}

void RowTable::first() {
    currentValue1 = currentValue2 = -1;
    previousValue1 = -1;
    next_pair();
}

void RowTable::mark() {
    BinaryTable::mark();
    markPreviousValue1 = previousValue1;
}

void RowTable::reset(const char i) {
    previousValue1 = markPreviousValue1;
    BinaryTable::reset(i);
}
