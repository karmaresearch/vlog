#include <trident/binarytables/fileindex.h>
#include <trident/binarytables/clustertable.h>

#include <trident/kb/consts.h>

#include <tridentcompr/utils/utils.h>

using namespace std;

ClusterTable::ClusterTable() {
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

}

void ClusterTable::next_pair() {
    //The purpose of "repeat" is that at the very beginning the iterator
    //is not set to ignore the second column, even the "ignore" flag is set to
    //true. With repeat we move to the end of the group and start reading the
    //correct next value
    const bool readNextFirstElement = isSecondColumnIgnored() && !shouldReadFirstTerm;

    if (shouldReadFirstTerm) {
        if (currentValue1 != -1) {
            previousFirstTerm = currentValue1;
        }

        advance();
        baseSecondTermFile = getCurrentFile();
        baseSecondTermPos = getCurrentPosition();

        currentValue1 = readFirstTerm();

        posLastFirstTerm = getCurrentPosition();
        fileLastFirstTerm = getCurrentFile();

        int flag = getByte();
        setDataStructuresBeforeNewSecondValue(flag);
    } else if (getCurrentPosition() == nextPosMark &&
               getCurrentFile() == nextFileMark) {
        previousSecondTerm = currentValue2;
        currentSecondTermIdx++;
        nextFileMark = secondTermIndex->file(currentSecondTermIdx);
        nextPosMark = getAbsoluteSecondTermPos(nextFileMark,
                                               secondTermIndex->pos(currentSecondTermIdx));
    }

    if (!readNextFirstElement) {
        currentValue2 = readSecondTerm();
        readBytes += getCurrentPosition() - positionSecondTerm;
        checkSecondGroupIsFinished();
    } else {
        //Move to the end of the group so that we can read the next first el.
        if (secondTermIndex != NULL) {
            int s = secondTermIndex->sizeIndex();
            short f = secondTermIndex->file(s - 1);
            int posFromIndex = secondTermIndex->pos(s - 1);
            int pos = getAbsoluteSecondTermPos(f, posFromIndex);
            this->setPosition(f, pos);
        } else {
            int moveToPos = getCurrentPosition() + remainingBytes - readBytes;
            if (getCurrentFileSize() <= moveToPos) {
                moveToPos -= getCurrentFileSize();
                setPosition(getCurrentFile() + 1, moveToPos);
            } else {
                setPosition(moveToPos);
            }
        }
        shouldReadFirstTerm = true;
        if (has_next())
            next_pair();
    }
}

void ClusterTable::setDataStructuresBeforeNewSecondValue(const int flag) {
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

int ClusterTable::getAbsoluteSecondTermPos(short file, int relPos) {
    if (file == baseSecondTermFile) {
        return baseSecondTermPos + relPos;
    } else {
        return relPos;
    }
}

bool ClusterTable::checkSecondGroupIsFinished() {
    if (secondTermIndex != NULL) {
        if (getCurrentPosition() == nextPosMark && getCurrentFile() == nextFileMark
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

void ClusterTable::first() {
    shouldReadFirstTerm = true;
    previousSecondTerm = currentValue1 = currentValue2 = nextPosMark =
            nextFileMark = -1;
    previousFirstTerm = currentSecondTermIdx = 0;
    removeSecondTermIndex = false;

    next_pair();
}

/*uint64_t ClusterTable::estimateNPairs() {
    if (remainingBytes == -1) {
        //Index
        return secondTermIndex->sizeIndex() * ADDITIONAL_SECOND_INDEX_SIZE;
    } else {
        return remainingBytes / Utils::numBytes2(value2());
    }
}*/

uint64_t ClusterTable::estNFirsts() {
    int space;
    if (getCurrentFile() == getLimitFile()) {
        space = getLimitPosition() - getCurrentPosition();
    } else {
        space = getFileSize(getCurrentFile()) - getCurrentPosition();
        space += getLimitPosition();
    }
    int els = space / 8;
    els = els / 3;
    if (els == 0)
        els = 1;
    return els;
}

uint64_t ClusterTable::getNFirsts() {
    long count = 1;
    long prevEl = currentValue1;
    while (has_next()) {
        next_pair();
        if (currentValue1 != prevEl) {
            prevEl = currentValue1;
            count++;
        }
    }
    return count;
}

uint64_t ClusterTable::estNSecondsFixedFirst() {
    return 5;
}

uint64_t ClusterTable::getNSecondsFixedFirst() {
    long count = 1;
    const long prevEl = currentValue1;
    long prevEl2 = currentValue2;
    while (has_next()) {
        next_pair();
        if (currentValue1 != prevEl) {
            break;
        } else {
            count++;
            prevEl2 = currentValue2;
        }
    }
    return count;
}

long ClusterTable::getCount() {
    long count = 1;
    considerSecondColumn();
    while (has_next() && !shouldReadFirstTerm) {
        next_pair();
        count++;
    }
    ignoreSecondColumn();
    return count;
}

long ClusterTable::readFirstTerm() {
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

long ClusterTable::readSecondTerm() {
    advance();

    // First determine which term to read
    positionSecondTerm = getCurrentPosition();
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

void ClusterTable::mode_difference(int modeValue1) {
    diffMode1 = modeValue1;
}

void ClusterTable::mode_compression(int compr1, int compr2) {
    this->compr1 = compr1;
    this->compr2 = compr2;
}

void ClusterTable::moveToClosestFirstTerm(long c1) {
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

    while (currentValue1 < c1 && has_next()) {
        if (currentValue1 != -1) {
            if (remainingBytes == -1) {
                int s = secondTermIndex->sizeIndex();
                short f = secondTermIndex->file(s - 1);
                int posFromIndex = secondTermIndex->pos(s - 1);
                int pos = getAbsoluteSecondTermPos(f, posFromIndex);
                this->setPosition(f, pos);
                shouldReadFirstTerm = true;
            } else {
                int moveToPos = getCurrentPosition() + remainingBytes - readBytes;
                if (getCurrentFileSize() <= moveToPos) {
                    moveToPos -= getCurrentFileSize();
                    setPosition(getCurrentFile() + 1, moveToPos);
                } else {
                    setPosition(moveToPos);
                }
                shouldReadFirstTerm = true;
            }
            if (!has_next()) {
                break;
            }
        }
        next_pair();
    }
}

void ClusterTable::moveToClosestSecondTerm(long c1, long c2) {
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
        endPosition = getCurrentPosition() + remainingBytes - readBytes;
    }

    if (currentValue2 < c2) {
        if (compr2 == COMPR_2) {
            /**** What if the list of elements is spread among more files?****/
            int file_size = getCurrentFileSize();

            if (getCurrentPosition() == file_size) {
                //This is a very rare case when an index entry points to the element after the last in the current file.
                //I advance to the next file and do the binary search from there
                setPosition(getCurrentFile() + 1, 0);
                if (endPosition >= file_size)
                    endPosition -= file_size;
            } else if (endPosition >= file_size
                       || (nextFileMark != -1 && nextFileMark != getCurrentFile())) {
                //Getting the very last part of the file
                int oPos = getCurrentPosition();
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
                    this->setPosition(getCurrentFile() + 1, 0);
                    if (endPosition >= file_size)
                        endPosition -= file_size;
                } else if (last_num > compare_with) {
                    this->setPosition(oPos);
                    endPosition = file_size;
                } else {
                    /** FOUND THE ELEMENT -> finish the function **/
                    this->setPosition(sPos + b_start_pos());
                    currentValue2 = readSecondTerm();
                    readBytes += getCurrentPosition() - oPos;
                    checkSecondGroupIsFinished();
                    return;
                }
            }

            // Now I have an interval in the same file
            int origPos = getCurrentPosition();
            int startPos = b_start_pos();

            //What if the interval is between two blocks?
            if (endPosition < startPos + b_len()) {
                int p = search_value(buffer(), origPos - startPos,
                                     endPosition - startPos,
                                     previousSecondTerm != -1 ?
                                     c2 - previousSecondTerm : c2);
                this->setPosition(p + startPos);
            } else { //This search is slightly slower but necessary
                int p = search_value_with_check(getCurrentPosition() , endPosition,
                                                previousSecondTerm != -1 ?
                                                c2 - previousSecondTerm : c2);
                this->setPosition(p);
            }

            if (getCurrentPosition() == endPosition) {
                currentValue1 = currentValue2 = LONG_MIN;
                //Set more_data() to fail
                setEndChunk();
                return;
            }

            currentValue2 = readSecondTerm();
            readBytes += getCurrentPosition() - origPos;
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

int ClusterTable::search_value(const char* b, int start, int end, const long value) {
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

int ClusterTable::search_value_with_check(int start, int end, const long value) {
    while (start < end) {
        setPosition((start + end) >> 1);
        while ((getByte() & 128) != 0 && (getCurrentPosition() < end))
            ;

        if (getCurrentPosition() < end) {
            //Read the value
            int start_pivot = getCurrentPosition();

            setPosition(start_pivot);
            long cv = getVLong2();
            if (cv < value) {
                start = getCurrentPosition();
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

void ClusterTable::mark() {
    BinaryTable::mark();
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
    //mSkipSecondColumn = skipSecondColumn;
}

void ClusterTable::reset(const char i) {
    BinaryTable::reset(i);
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
    //skipSecondColumn = mSkipSecondColumn;
}
