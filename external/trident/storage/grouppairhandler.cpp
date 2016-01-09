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

#include <trident/storage/fileindex.h>
#include <trident/storage/pairhandler.h>

#include <trident/kb/consts.h>

#include <tridentcompr/utils/utils.h>

using namespace std;

GroupPairHandler::GroupPairHandler() {
    previousFirstTerm = previousSecondTerm = 0;
    baseSecondTermFile = 0;
    baseSecondTermPos = 0;
    fileLastFirstTerm = 0;
    posLastFirstTerm = 0;
    secondTermIndex = NULL;
    removeSecondTermIndex = false;

    shouldReadFirstTerm = true;
    readBytes = 0;
    remainingBytes = 0;
    nextFileMark = 0;
    nextPosMark = 0;
    currentSecondTermIdx = 0;

    positionSecondTerm = 0;
    diffMode1 = 0;
    compr1 = compr2 = 0;

    mPreviousFirstTerm = mPreviousSecondTerm = 0;
    mBaseSecondTermFile = 0;
    mBaseSecondTermPos = 0;
    mFileLastFirstTerm = 0;
    mPosLastFirstTerm = 0;
    mSecondTermIndex = NULL;
    mShouldReadFirstTerm = true;
    mReadBytes = 0;
    mRemainingBytes = 0;
    mNextFileMark = 0;
    mNextPosMark = 0;
    mCurrentSecondTermIdx = 0;
    mPositionSecondTerm = 0;

    lastSecondTerm = 0;
    nElementsForIndexing = 0;
    bytesUsed = 0;
    smallGroupMode = true;
    nElementsGroup = 0;
}

void GroupPairHandler::next_pair() {
    if (shouldReadFirstTerm) {
        if (currentValue1 != -1) {
            previousFirstTerm = currentValue1;
        }

        advance();
        // First determine which term to write
        baseSecondTermFile = currentFile;
        baseSecondTermPos = currentPos;

        currentValue1 = readFirstTerm();

        posLastFirstTerm = currentPos;
        fileLastFirstTerm = currentFile;

        int flag = getByte();
        setDataStructuresBeforeNewSecondValue(flag);
    } else if (currentPos == nextPosMark && currentFile == nextFileMark) {
        previousSecondTerm = currentValue2;
        currentSecondTermIdx++;
        nextFileMark = secondTermIndex->file(currentSecondTermIdx);
        nextPosMark = getAbsoluteSecondTermPos(nextFileMark,
                                               secondTermIndex->pos(currentSecondTermIdx));
    }

    currentValue2 = readSecondTerm();
    readBytes += currentPos - positionSecondTerm;

    checkSecondGroupIsFinished();
}

void GroupPairHandler::setDataStructuresBeforeNewSecondValue(const int flag) {
    if (flag == 0) {
        secondTermIndex = index->additional_idx(currentValue1);
        remainingBytes = -1;
        nextFileMark = secondTermIndex->file(0);
        nextPosMark = getAbsoluteSecondTermPos(nextFileMark,
                                               secondTermIndex->pos(0));
        currentSecondTermIdx = 0;
    } else {
        remainingBytes = flag & 255;
        secondTermIndex = NULL;
        nextFileMark = -1;
        nextPosMark = -1;
    }
    previousSecondTerm = -1;
    readBytes = 0;
    shouldReadFirstTerm = false;
}

int GroupPairHandler::getAbsoluteSecondTermPos(short file, int relPos) {
    if (file == baseSecondTermFile) {
        return baseSecondTermPos + relPos;
    } else {
        return relPos;
    }
}

bool GroupPairHandler::checkSecondGroupIsFinished() {
    if (secondTermIndex != NULL) {
        if (currentPos == nextPosMark && currentFile == nextFileMark
                && currentSecondTermIdx == secondTermIndex->sizeIndex() - 1) {
            shouldReadFirstTerm = true;
        }
    } else {
        if (remainingBytes == readBytes) {
            shouldReadFirstTerm = true;
        }
    }
    return shouldReadFirstTerm;
}

void GroupPairHandler::start_reading() {
    shouldReadFirstTerm = true;
    previousSecondTerm = currentValue1 = currentValue2 = nextPosMark =
            nextFileMark = -1;
    previousFirstTerm = currentSecondTermIdx = 0;
    removeSecondTermIndex = false;
}

long GroupPairHandler::readFirstTerm() {
    long term;
    if (compr1 == COMPR_1) {
        term = getVLong();
    } else if (compr1 == COMPR_2) {
        term = getVLong2();
    } else {
        term = getLong();
    }
    if (diffMode1 == DIFFERENCE && currentValue1 != -1) {
        term += currentValue1;
    }
    return term;
}

long GroupPairHandler::readSecondTerm() {
    advance();

    // First determine which term to read
    positionSecondTerm = currentPos;
    long term;
    if (compr2 == COMPR_1) {
        term = getVLong();
    } else if (compr2 == COMPR_2) {
        term = getVLong2();
    } else {
        term = getLong();
    }

    if (previousSecondTerm != -1) {
        term += previousSecondTerm;
    } else {
        previousSecondTerm = term;
    }

    return term;
}

void GroupPairHandler::mode_difference(int modeValue1) {
    diffMode1 = modeValue1;
}

void GroupPairHandler::mode_compression(int compr1, int compr2) {
    this->compr1 = compr1;
    this->compr2 = compr2;
}

void GroupPairHandler::moveToClosestFirstTerm(long c1) {
    if (index != NULL && index->sizeIndex() > 0) {
        int pos = index->idx(c1);
        if (pos > 0) {
            pos--;

            // Move the cursor to the file/pos
            setRelativePosition(index->file(pos), index->pos(pos));
            currentValue1 = index->key(pos);
            shouldReadFirstTerm = true;
            next_pair();
        }
    }

    while (currentValue1 < c1 && more_data()) {
        if (currentValue1 != -1) {
            if (remainingBytes == -1) {
                int s = secondTermIndex->sizeIndex();
                short f = secondTermIndex->file(s - 1);
                int posFromIndex = secondTermIndex->pos(s - 1);
                int pos = getAbsoluteSecondTermPos(f, posFromIndex);
                this->setPosition(f, pos);
                shouldReadFirstTerm = true;
            } else {
                int moveToPos = currentPos + remainingBytes - readBytes;
                if (get_current_file_size() <= moveToPos) {
                    moveToPos -= get_current_file_size();
                    setPosition(currentFile + 1, moveToPos);
                } else {
                    setPosition(moveToPos);
                }
                shouldReadFirstTerm = true;
            }
            if (!more_data()) {
                break;
            }
        }
        next_pair();
    }
}

void GroupPairHandler::moveToClosestSecondTerm(long c1, long c2) {
    int endPosition = -1;
    if (secondTermIndex != NULL) {
        int idx = secondTermIndex->idx(currentSecondTermIdx, c2);
        if (idx >= secondTermIndex->sizeIndex()) {
            // The value is too large too exists. Give it a fake value
            currentValue1 = currentValue2 = LONG_MIN;
            //Set more_data() to fail
            setEndChunk();
            return;
        }

        // Update the coordinate for the higher mark
        nextFileMark = secondTermIndex->file(idx);
        nextPosMark = getAbsoluteSecondTermPos(nextFileMark,
                                               secondTermIndex->pos(idx));
        currentSecondTermIdx = idx;

        // Set the higher bound
        endPosition = nextPosMark;

        if (idx > 0) {
            // Move the pointer to the correct location
            idx--;
            setPosition(secondTermIndex->file(idx),
                        getAbsoluteSecondTermPos(secondTermIndex->file(idx),
                                                 secondTermIndex->pos(idx)));
            currentValue2 = previousSecondTerm = secondTermIndex->key(idx);
        }
    } else {
        endPosition = currentPos + remainingBytes - readBytes;
    }

    if (currentValue2 < c2) {
        if (compr2 == COMPR_2) {
            /**** What if the list of elements is spread among more files?****/
            int file_size = get_current_file_size();

            if (currentPos == file_size) {
                //This is a very rare case when an index entry points to the element after the last in the current file.
                //I advance to the next file and do the binary search from there
                setPosition(currentFile + 1, 0);
                if (endPosition >= file_size)
                    endPosition -= file_size;
            } else if (endPosition >= file_size
                       || (nextFileMark != -1 && nextFileMark != currentFile)) {
                //Getting the very last part of the file
                int oPos = currentPos;
                this->setPosition(file_size - BLOCK_MIN_SIZE);
                int sPos = file_size - b_start_pos() - 2;
                while ((buffer()[sPos] & 128) != 0) {
                    sPos--;
                }
                sPos++;

                //sPos contains the starting point of the last number
                int tmpPos = sPos;
                long last_num = Utils::decode_vlong2(buffer(), &tmpPos);
                long compare_with =
                    previousSecondTerm != -1 ? c2 - previousSecondTerm : c2;
                if (last_num < compare_with) {
                    this->setPosition(currentFile + 1, 0);
                    if (endPosition >= file_size)
                        endPosition -= file_size;
                } else if (last_num > compare_with) {
                    this->setPosition(oPos);
                    endPosition = file_size;
                } else {
                    /** FOUND THE ELEMENT -> finish the function **/
                    this->setPosition(sPos + b_start_pos());
                    currentValue2 = readSecondTerm();
                    readBytes += currentPos - oPos;
                    checkSecondGroupIsFinished();
                    return;
                }
            }

            // Now I have an interval in the same file
            int origPos = currentPos;
            int startPos = b_start_pos();

            //What if the interval is between two blocks?
            if (endPosition < startPos + b_len()) {
                int p = search_value(buffer(), origPos - startPos,
                                     endPosition - startPos,
                                     previousSecondTerm != -1 ?
                                     c2 - previousSecondTerm : c2);
                this->setPosition(p + startPos);
            } else { //This search is slightly slower but necessary
                int p = search_value_with_check(currentPos, endPosition,
                                                previousSecondTerm != -1 ?
                                                c2 - previousSecondTerm : c2);
                this->setPosition(p);
            }

            if (currentPos == endPosition) {
                currentValue1 = currentValue2 = LONG_MIN;
                //Set more_data() to fail
                setEndChunk();
                return;
            }

            currentValue2 = readSecondTerm();
            readBytes += currentPos - origPos;
            checkSecondGroupIsFinished();
        } else if (compr2 == NO_COMPR) {
            //I can do binary search, since all numbers have the same size.
            //I don't implement it because with the current strategy this should never happen.
            BOOST_LOG_TRIVIAL(error) << "Should not occur";
            throw 10;
        }
    }

    // From now on, we search with linear search
    while (!shouldReadFirstTerm && currentValue2 < c2) {
        next_pair();
    }
}

int GroupPairHandler::search_value(const char* b, int start, int end, const long value) {
    while (start < end) {
        int pivot = (start + end) >> 1;
        while ((b[pivot++] & 128) != 0 && (pivot < end))
            ;
        if (pivot < end) {
            //Read the value
            int start_pivot = pivot;
            long cv = Utils::decode_vlong2(b, &pivot);
            if (cv < value) {
                start = pivot;
            } else if (cv > value) {
                end = start_pivot;
            } else {
                return start_pivot;
            }
        } else {
            return start;
        }
    }
    return start;
}

int GroupPairHandler::search_value_with_check(int start, int end, const long value) {
    while (start < end) {
        setPosition((start + end) >> 1);
        while ((getByte() & 128) != 0 && (currentPos < end))
            ;

        if (currentPos < end) {
            //Read the value
            int start_pivot = currentPos;

            setPosition(start_pivot);
            long cv = getVLong2();
            if (cv < value) {
                start = currentPos;
            } else if (cv > value) {
                end = start_pivot;
            } else {
                return start_pivot;
            }
        } else {
            return start;
        }
    }
    return start;
}

void GroupPairHandler::mark() {
    PairHandler::mark();
    mPreviousFirstTerm = previousFirstTerm;
    mPreviousSecondTerm = previousSecondTerm;
    mBaseSecondTermFile = baseSecondTermFile;
    mBaseSecondTermPos = baseSecondTermPos;
    mFileLastFirstTerm = fileLastFirstTerm;
    mPosLastFirstTerm = posLastFirstTerm;
    mSecondTermIndex = secondTermIndex;
    mShouldReadFirstTerm = shouldReadFirstTerm;
    mReadBytes = readBytes;
    mRemainingBytes = remainingBytes;
    mNextFileMark = nextFileMark;
    mNextPosMark = nextPosMark;
    mCurrentSecondTermIdx = currentSecondTermIdx;
    mPositionSecondTerm = positionSecondTerm;
}

void GroupPairHandler::reset() {
    PairHandler::reset();
    previousFirstTerm = mPreviousFirstTerm;
    previousSecondTerm = mPreviousSecondTerm;
    baseSecondTermFile = mBaseSecondTermFile;
    baseSecondTermPos = mBaseSecondTermPos;
    fileLastFirstTerm = mFileLastFirstTerm;
    posLastFirstTerm = mPosLastFirstTerm;
    secondTermIndex = mSecondTermIndex;
    shouldReadFirstTerm = mShouldReadFirstTerm;
    readBytes = mReadBytes;
    remainingBytes = mRemainingBytes;
    nextFileMark = mNextFileMark;
    nextPosMark = mNextPosMark;
    currentSecondTermIdx = mCurrentSecondTermIdx;
    positionSecondTerm = mPositionSecondTerm;
}

void GroupPairHandler::startAppend() {
    previousFirstTerm = previousSecondTerm = lastSecondTerm = -1;
    posLastFirstTerm = fileLastFirstTerm = -1;
    nElementsForIndexing = 0;
    if (secondTermIndex == NULL) {
        secondTermIndex = new FileIndex();
        removeSecondTermIndex = true;
    }
    baseSecondTermFile = getCurrentFile();
    baseSecondTermPos = getCurrentPosition();
}

void GroupPairHandler::append(long t1, long t2) {
    if (t1 != previousFirstTerm) {
        if (previousFirstTerm != -1) {
//          increaseIfNecessary();
            insertSecondTerm(true);

            if (smallGroupMode) {
                // Write previous count of elements
                writeNElementsAt((char) bytesUsed, fileLastFirstTerm,
                                 posLastFirstTerm);
            } else {
                // Add one last position to the index to mark the end of the
                // group
                if (nElementsGroup != 0) {
                    secondTermIndex->add(lastSecondTerm, getCurrentFile(),
                                         getRelativeSecondTermPos());
                }

                // Add the additional index to the primary index
                index->addAdditionalIndex(previousFirstTerm, secondTermIndex);
                // Prepare an index for this second range
                secondTermIndex = new FileIndex();
                removeSecondTermIndex = true;
            }
            baseSecondTermFile = getCurrentFile();
            baseSecondTermPos = getCurrentPosition();

            // Reset the variables
            lastSecondTerm = previousSecondTerm = -1;
        }

        bytesUsed = 0;
        smallGroupMode = true;
        nElementsGroup = 0;

        updateFirstTermIndex(previousFirstTerm);
//      increaseIfNecessary();
        writeFirstTerm(calculateFirstTermToWrite(t1));
        previousFirstTerm = t1;

        posLastFirstTerm = getCurrentPosition();
        fileLastFirstTerm = getCurrentFile();
        writeByte(0);
        nElementsForIndexing++;
    } else {
//      increaseIfNecessary();
        insertSecondTerm(false);
    }
//
    lastSecondTerm = t2;
    nElementsGroup++;

}

void GroupPairHandler::stopAppend() {
    if (lastSecondTerm != -1) {
        insertSecondTerm(true);
        if (smallGroupMode) {
            // Write previous count of elements
            writeNElementsAt((char) bytesUsed, fileLastFirstTerm, posLastFirstTerm);
        } else {
            // Add one last position to the index to mark the end of the
            // group. In nElementsGroup=0, then the previous call to
            // updateSecondTermIndex has insert an entry (therefore we do
            // not add it to prevent a double entry)

            if (nElementsGroup != 0) {
                secondTermIndex->add(lastSecondTerm, getCurrentFile(),
                                     getRelativeSecondTermPos());
            }

            // Add the additional index to the primary index
            index->addAdditionalIndex(previousFirstTerm, secondTermIndex);
            secondTermIndex = NULL;
        }
    }
}

int GroupPairHandler::getRelativeSecondTermPos() {
    if (getCurrentFile() == baseSecondTermFile) {
        return getCurrentPosition() - baseSecondTermPos;
    } else {
        return getCurrentPosition();
    }
}

int GroupPairHandler::getRelativeSecondTermPos(short file, int absPos) {
    if (file == baseSecondTermFile) {
        return absPos - baseSecondTermPos;
    } else {
        return absPos;
    }
}

void GroupPairHandler::writeNElementsAt(char b, short file, int pos) {
    // Write the number of elements
    overwriteBAt(b, file, pos);
}

void GroupPairHandler::insertSecondTerm(bool last) {
    int nbytes = writeSecondTerm(calculateSecondTermToWrite(lastSecondTerm));
    updateSecondTermIndex(lastSecondTerm, nbytes, getCurrentFile(),
                          getCurrentPosition());
}

void GroupPairHandler::updateSecondTermIndex(long lastTermWritten,
        int bytesTaken, short currentFile, int currentPos) {
    if (smallGroupMode) {
        if (bytesUsed + bytesTaken > 255) {
            smallGroupMode = false;
            nElementsGroup = 0;
            previousSecondTerm = lastTermWritten;
            secondTermIndex->add(lastTermWritten, currentFile,
                                 getRelativeSecondTermPos(currentFile, currentPos));
        } else {
            bytesUsed += bytesTaken;
        }
    } else {
        if (nElementsGroup == ADDITIONAL_SECOND_INDEX_SIZE) {
            secondTermIndex->add(lastTermWritten, currentFile,
                                 getRelativeSecondTermPos(currentFile, currentPos));
            previousSecondTerm = lastTermWritten;
            nElementsGroup = 0;
        }
    }
}

long GroupPairHandler::calculateSecondTermToWrite(long term) {
    // First calculate the term to write
    if (previousSecondTerm == -1) {
        previousSecondTerm = term;
    } else {
        term -= previousSecondTerm;
    }
    return term;
}

int GroupPairHandler::writeSecondTerm(long termToWrite) {
    if (compr2 == COMPR_1) {
        return writeVLong(termToWrite);
    } else if (compr2 == COMPR_2) {
        return writeVLong2(termToWrite);
    } else {
        return writeLong(termToWrite);
    }
}

void GroupPairHandler::updateFirstTermIndex(const long t1) {
    // Should a write an entry in the index?
    if (nElementsForIndexing >= FIRST_INDEX_SIZE && previousFirstTerm != -1) {
        nElementsForIndexing = 0;
        index->add(t1, getCurrentFile(), getRelativePosition());
    }
}

void GroupPairHandler::writeFirstTerm(long termToWrite) {
    if (compr1 == COMPR_1) {
        writeVLong(termToWrite);
    } else if (compr1 == COMPR_2) {
        writeVLong2(termToWrite);
    } else {
        writeLong(termToWrite);
    }
}

long GroupPairHandler::calculateFirstTermToWrite(long termToWrite) {
    // First determine which term to write
    if (diffMode1 == DIFFERENCE && previousFirstTerm != -1) {
        termToWrite -= previousFirstTerm;
    }
    return termToWrite;
}

uint64_t GroupPairHandler::estimateNPairs() {
    if (remainingBytes == -1) {
        //Index
        return secondTermIndex->sizeIndex() * ADDITIONAL_SECOND_INDEX_SIZE;
    } else {
        return remainingBytes / Utils::numBytes2(value2());
    }
}

