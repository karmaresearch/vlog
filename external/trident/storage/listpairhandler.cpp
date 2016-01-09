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

#include <trident/storage/pairhandler.h>

void ListPairHandler::setCompressionMode(int v1, int v2) {
    comprValue1 = v1;
    comprValue2 = v2;
}

void ListPairHandler::setDifferenceMode(int d1) {
    diffValue1 = d1;
}

void ListPairHandler::next_pair() {
    // Record the previous values and coordinates
    advance();

    if (currentValue1 != -1)
        previousValue1 = currentValue1;

    currentValue1 = readFirstTerm();

    // Read second element
    advance();
    currentValue2 = readSecondTerm();
}

long ListPairHandler::readFirstTerm() {
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

long ListPairHandler::readSecondTerm() {
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

void ListPairHandler::moveToClosestFirstTerm(long c1) {
    if (currentValue1 >= c1 || !more_data()) {
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
        if (currentValue1 < c1 && more_data()) {
            //First calculate the end file and position
            short endFile = 0;
            int endPos = 0;
            if (index == NULL || posIndex == index->sizeIndex() - 1) {
                endPos = limitPos;
                endFile = limitFile;
            } else {
                endFile = index->file(posIndex + 1);
                int indexPos = index->pos(posIndex + 1);
                endPos = getAbsPosition(endFile, indexPos);
            }

            /* This if contains code for a special case if the
             * interval is between two files
             */
            if (endFile != currentFile) {
                //Check the last value of the file to decide which file
                //we should search on...
                int oPos = currentPos;

                //Determine the position of the element
                int fileSize = get_current_file_size();
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
                    endFile = currentFile;
                    endPos = fileSize;
                } else {
                    currentValue1 = last_num;
                    advance();
                    currentValue2 = readSecondTerm();
                    return;
                }
            }

            //Now I now the entire range to perform a binary search...
            int beginPos = currentPos;
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

            while (currentValue1 < c1 && more_data()) {
                next_pair();
            }
        }
    } else {
        // From now on, just continue with a linear scan
        while (currentValue1 < c1 && more_data()) {
            next_pair();
        }
    }
}

void ListPairHandler::moveToClosestSecondTerm(long c1, long c2) {
    // Only linear search is possible...
    while (currentValue1 == c1 && currentValue2 < c2 && more_data()) {
        next_pair();
    }
}

void ListPairHandler::start_reading() {
    currentValue1 = currentValue2 = -1;
    previousValue1 = -1;
}

void ListPairHandler::mark() {
    PairHandler::mark();
    markPreviousValue1 = previousValue1;
}

void ListPairHandler::reset() {
    previousValue1 = markPreviousValue1;
    PairHandler::reset();
}

void ListPairHandler::startAppend() {
    previousValue1 = 0;
    nElements = 0;
}

void ListPairHandler::append(long t1, long t2) {
    bool writeIndexEntry = false;
    long keyToStore = 0;
    if (t1 != previousValue1 && nElements >= FIRST_INDEX_SIZE) {
        keyToStore = previousValue1;
        writeIndexEntry = true;
        nElements = 0;
    }

    // Write the index entry
    if (writeIndexEntry) {
        index->add(keyToStore, getCurrentFile(), getRelativePosition());
    }

    if (diffValue1 == DIFFERENCE) {
        t1 -= previousValue1;
        previousValue1 += t1;
    } else {
        previousValue1 = t1;
    }

    writeFirstTerm(t1);
    writeSecondTerm(t2);
    nElements++;
}

void ListPairHandler::stopAppend() {
}

void ListPairHandler::writeFirstTerm(long t) {
    switch (comprValue1) {
    case COMPR_1:
        writeVLong(t);
        return;
    case COMPR_2:
        writeVLong2(t);
        return;
    case NO_COMPR:
        writeLong(t);
        return;
    }
}

void ListPairHandler::writeSecondTerm(long t) {
    switch (comprValue2) {
    case COMPR_1:
        writeVLong(t);
        return;
    case COMPR_2:
        writeVLong2(t);
        return;
    case NO_COMPR:
        writeLong(t);
    }
}

uint64_t ListPairHandler::estimateNPairs() {
    return 1;
}
