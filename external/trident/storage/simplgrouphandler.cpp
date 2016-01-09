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

//#include <trident/model/column.h>

void SimplifiedGroupPairHandler::start_reading() {
    advance();
    onlyFirstValues = false;
    currentValue1 = firstValue1 = readFirstTerm();

    if (index != NULL) {
        secondTermIndex = index->additional_idx(currentValue1);
    } else {
        secondTermIndex = NULL;
    }
    lastSecondTermIndex = 0;
    if (secondTermIndex == NULL || secondTermIndex->sizeIndex() == 0) {
        lastSecondTermIndexPos = SIZE_MAX;
    } else {
        lastSecondTermIndexPos = secondTermIndex->pos(0);
    }

    const uint8_t flg = (uint8_t) getByte();
    idxNextPointer = 0;
    if (flg) {
        bytesPerFirstEntry = flg >> 4;
        bytesPerPointer = flg & 15;
        nFirstEntries = getVLong2();

        startfile1 = endfile1 = getCurrentFile();
        startpos1 = getCurrentPosition();
        endpos1 = startpos1 + (nFirstEntries - 1) * (bytesPerFirstEntry + bytesPerPointer);
        endfile1 = startfile1;
        while (endpos1 >= getFileSize(endfile1)) {
            endpos1 -= getFileSize(endfile1);
            endfile1++;
        }
        setPosition(endfile1, endpos1);

        startfile2 = endfile1;
        startpos2 = endpos1;

        endfile2 = startfile2;
        setPosition(startfile1, startpos1);
        endpos2 = startpos2 + getVLong2();
        while (endpos2 > getFileSize(endfile2)) {
            endpos2 -= getFileSize(endfile2);
            endfile2++;
        }
    } else {
        bytesPerFirstEntry = bytesPerPointer = 0;
        nFirstEntries = 1;

        startfile2 = startfile1 = endfile1 = getCurrentFile();
        startpos2 = startpos1 = endpos1 = getCurrentPosition();
        endfile2 = getLimitFile();
        endpos2 = getLimitPosition();
    }

    //Set the position to the beginning of the first second element
    setPosition(startfile2, startpos2);
}

void SimplifiedGroupPairHandler::next_pair() {
    if (!more_data()) {
        return;
    }

    advance();

    //If pos==index then move to the next first value
    if (idxNextPointer < nFirstEntries - 1 && ((getCurrentFile() == endfile2 &&
            getCurrentPosition() == endpos2) || onlyFirstValues)) {
        short origFile =
            getCurrentFile();
        size_t origPos = getCurrentPosition();

        //Go back to read the next first term
        short fileNextFirstTerm = startfile1;
        uint64_t posNextFirstTerm = startpos1 + bytesPerPointer + (bytesPerPointer + bytesPerFirstEntry) * idxNextPointer;
        while (posNextFirstTerm >= getFileSize(fileNextFirstTerm)) {
            posNextFirstTerm -= getFileSize(fileNextFirstTerm);
            fileNextFirstTerm++;
        }
        setPosition(fileNextFirstTerm, posNextFirstTerm);
        currentValue1 = readFirstTerm() + firstValue1;

        idxNextPointer++;

        if (!onlyFirstValues) {
            if (idxNextPointer < nFirstEntries - 1) {
                //Read the limit for the next group
                short filePointer = fileNextFirstTerm;
                uint64_t posPointer = posNextFirstTerm + bytesPerFirstEntry;
                while (posPointer >= getFileSize(filePointer)) {
                    posPointer -= getFileSize(filePointer);
                    filePointer++;
                }
                setPosition(filePointer, posPointer);

                endfile2 = endfile1;
                endpos2 = endpos1 + getVLong2();
                while (endpos2 >= getFileSize(endfile2)) {
                    endpos2 -= getFileSize(endfile2);
                    endfile2++;
                }
            } else {
                endfile2 = getLimitFile();
                endpos2 = getLimitPosition();
            }

            if (index != NULL) {
                secondTermIndex = index->additional_idx(currentValue1);
            } else {
                secondTermIndex = NULL;
            }
            lastSecondTermIndex = 0;
            if (secondTermIndex == NULL || secondTermIndex->sizeIndex() == 0) {
                lastSecondTermIndexPos = SIZE_MAX;
            } else {
                lastSecondTermIndexPos = secondTermIndex->pos(0);
            }
            setPosition(origFile, origPos);
            startfile2 = origFile;
            startpos2 = origPos;
            advance();
        }
    } else if (onlyFirstValues) {
        //The element before was the last one. Move to the end of the segment
        setPosition(getLimitFile(), getLimitPosition());
    }

    if (onlyFirstValues)
        return;

    //Read second value
    if (getCurrentPosition() == startpos2) {
        firstValue2 = currentValue2 = readSecondTerm();
    } else {
        if (getCurrentPosition() == lastSecondTermIndexPos) {
            firstValue2 = secondTermIndex->key(lastSecondTermIndex);
            lastSecondTermIndex++;
            if (lastSecondTermIndex < secondTermIndex->sizeIndex()) {
                lastSecondTermIndexPos = secondTermIndex->pos(lastSecondTermIndex);
            } else {
                lastSecondTermIndexPos = SIZE_MAX;
            }
        }
        currentValue2 = readSecondTerm() + firstValue2;
    }
}

void SimplifiedGroupPairHandler::ignoreSecondColumn() {
    onlyFirstValues = true;

    //Move the pointer to the current first entry
    short fileNextFirstTerm = startfile1;
    uint64_t posNextFirstTerm = startpos1 + bytesPerPointer +
                                (bytesPerPointer + bytesPerFirstEntry) * idxNextPointer;
    while (posNextFirstTerm >= getFileSize(fileNextFirstTerm)) {
        posNextFirstTerm -= getFileSize(fileNextFirstTerm);
        fileNextFirstTerm++;
    }
    setPosition(fileNextFirstTerm, posNextFirstTerm);

    //Set new limit
    setLimitPosition(endfile1, endpos1);
}

void SimplifiedGroupPairHandler::setGroup(const short file, const size_t pos, const size_t blockSize) {
    if (index != NULL) {
        secondTermIndex = index->additional_idx(currentValue1);
    } else {
        secondTermIndex = NULL;
    }
    lastSecondTermIndex = 0;
    if (secondTermIndex == NULL || secondTermIndex->sizeIndex() == 0) {
        lastSecondTermIndexPos = SIZE_MAX;
    } else {
        lastSecondTermIndexPos = secondTermIndex->pos(0);
    }

    //Set pos next group
    size_t beginningRangeInFile = startpos1;
    idxNextPointer = 0;
    short firstFile = startfile1;
    while (file > firstFile) {
        //Need to add all ranges in the previous files
        idxNextPointer += (getFileSize(firstFile) - beginningRangeInFile) / blockSize;
        beginningRangeInFile = 0;
        firstFile++;
    }
    idxNextPointer += ((pos - beginningRangeInFile) / blockSize) + 1;
    assert(idxNextPointer > 0);
    if (idxNextPointer < nFirstEntries - 1) {
        //In which file is this?
        short startFileNextPointer = file;
        size_t startPosNextPointer = pos + blockSize;
        while (startPosNextPointer >= getFileSize(startFileNextPointer)) {
            startPosNextPointer -= getFileSize(startFileNextPointer);
            startFileNextPointer++;
        }
        setPosition(startFileNextPointer, startPosNextPointer);

        endfile2 = endfile1;
        endpos2 = endpos1 + getVLong2();
        while (endpos2 > getFileSize(endfile2)) {
            endpos2 -= getFileSize(endfile2);
            endfile2++;
        }
    } else {
        endfile2 = getLimitFile();
        endpos2 = getLimitPosition();
    }

    //Move to second pair, and read it.
    short readFile2 = file;
    size_t readPos2 = pos;
    while (readPos2 >= getFileSize(readFile2)) {
        readPos2 -= getFileSize(readFile2);
        readFile2++;
    }
    setPosition(readFile2, readPos2);
    startfile2 = endfile1;
    startpos2 = endpos1 + getVLong2();
    while (startpos2 >= getFileSize(startfile2)) {
        startpos2 -= getFileSize(startfile2);
        startfile2++;
    }
    setPosition(startfile2, startpos2);
    currentValue2 = firstValue2 = readSecondTerm();
}

void SimplifiedGroupPairHandler::moveToClosestFirstTerm(long c1) {
    if (c1 == firstValue1) {
        //Set endfile2,endpos2
        if (nFirstEntries == 1) {
            endfile2 = getLimitFile();
            endpos2 = getLimitPosition();
        } else {
            setPosition(startfile1, startpos1);
            endfile2 = endfile1;
            endpos2 = endpos1 + getVLong2();
            while (endpos2 > getFileSize(endfile2)) {
                endpos2 -= getFileSize(endfile2);
                endfile2++;
            }
        }
        currentValue1 = firstValue1;
        setPosition(endfile1, endpos1);
        firstValue2 = currentValue2 = readSecondTerm();
        startfile2 = endfile1;
        startpos2 = endpos1;

        if (index != NULL) {
            secondTermIndex = index->additional_idx(currentValue1);
        } else {
            secondTermIndex = NULL;
        }
        lastSecondTermIndex = 0;
        if (secondTermIndex == NULL || secondTermIndex->sizeIndex() == 0) {
            lastSecondTermIndexPos = SIZE_MAX;
        } else {
            lastSecondTermIndexPos = secondTermIndex->pos(0);
        }
        return;
    }

    if (c1 == currentValue1) {
        //In this case, I must read the first second value
        setPosition(startfile2, startpos2);
        firstValue2 = currentValue2 = readSecondTerm();
        lastSecondTermIndex = 0;
        if (secondTermIndex == NULL || secondTermIndex->sizeIndex() == 0) {
            lastSecondTermIndexPos = SIZE_MAX;
        } else {
            lastSecondTermIndexPos = secondTermIndex->pos(0);
        }
        return;
    }

    short sf = startfile1;
    size_t sp = startpos1;
    short ef = endfile1;
    size_t ep = endpos1;

    if (index != NULL && index->sizeIndex() > 0) {
        int pos = index->idx(c1);
        if (pos > 0) {
            sf = index->file(pos - 1);
            sp = index->pos(pos - 1);
            setPosition(sf, sp);
        }
        if (pos < index->sizeIndex()) {
            ep = index->pos(pos);
            ef = index->file(pos);
        }
    }

    //Adjust the search space if it is among two different files
    if (sf != ef) {
        //If they are still different, then the difference must be of a single file,
        //and there must be something to read in the second file
        setPosition(ef, bytesPerPointer);
        long firstValueInFile = firstValue1 + readFirstTerm();
        if (firstValueInFile <= c1) {
            sf = ef;
            sp = 0;
        } else {
            ef = sf;
            ep = getFileSize(sf);
        }
        setPosition(sf, sp);
    }

    //Search among the first elements
    long elToSeach = c1 - firstValue1;
    size_t blockSize = bytesPerFirstEntry + bytesPerPointer;
    bool setPos = sp < ep;
    size_t middle;
    while (sp < ep) {
        middle = sp + (ep - sp) / blockSize / 2 * blockSize;
        setPosition(sf, middle + bytesPerPointer);
        long v = readFirstTerm();
        currentValue1 = v + firstValue1;
        if (v == elToSeach) { //Found
            setGroup(sf, middle, blockSize);
            return;
        } else if (v < elToSeach) {
            sp = middle + blockSize;
        } else {
            ep = middle;
        }
    }

    if (setPos) {
        //middle is the last value read
        setGroup(sf, middle, blockSize);
    }

    //Value not found. second term is undefined.
    while (more_data() && currentValue1 < c1) {
        next_pair();
    }
}

void SimplifiedGroupPairHandler::moveToClosestSecondTerm(long c1, long c2) {
    assert(!onlyFirstValues);
    if (c2 <= currentValue2) {
        return;
    }

    if (compr2 == COMPR_1) {
        //Only linear search is possible
        while (more_data() && currentValue2 < c2) {
            next_pair();
        }
    } else if (compr2 == COMPR_2) {
        //Search from current position to the end of the fragment
        short s_file = getCurrentFile();
        int s = getCurrentPosition();

        int e = endpos2;
        short e_file = endfile2;
        if (secondTermIndex != NULL) {
            int idx = secondTermIndex->idx(lastSecondTermIndex, c2);
            if (idx < secondTermIndex->sizeIndex()) {
                e_file = secondTermIndex->file(idx);
                e = secondTermIndex->pos(idx);
                lastSecondTermIndex = idx;
                lastSecondTermIndexPos = secondTermIndex->pos(idx);
            }
            if (idx > 0) {
                s = secondTermIndex->pos(idx - 1);
                s_file = secondTermIndex->file(idx - 1);
                firstValue2 = secondTermIndex->key(idx - 1);
                setPosition(s_file, s);
            } else {
                //s = startpos2;
                //s_file = startfile2;
                //setPosition(s_file, s);
            }
        }

        if (s_file != e_file) {
            //Check the first value of the file
            setPosition(e_file, 0);
            long n = getVLong2();
            if (c2 >= firstValue2 + n) {
                s_file++;
                s = 0;
            } else {
                e_file--;
                e = getFileSize(e_file);
            }
            setPosition(s_file, s);
        }

        //At this point the range is on the same file
        if (e - s >= MIN_SIZE_BINARYSEARCH2 && c2 > currentValue2 + 3) {
            long valueToSearch = c2 - firstValue2;
            size_t startPos = b_start_pos();
            if (e < startPos + b_len()) {
                size_t ss = s - startPos;
                size_t se = e - startPos;
                size_t offset = GroupPairHandler::search_value(buffer(), ss,
                                se, valueToSearch);
                if (offset < se) {
                    setPosition(startPos + offset);
                    if (getVLong2() == valueToSearch) {
                        currentValue2 = c2;
                        return;
                    }
                } else {
                    setPosition(e);
                }
            } else {
                while (s < e) {
                    int middle = (s + e) / 2;
                    // Go back until you find beginning of a value
                    setPosition(middle);
                    while (middle > s) {
                        setPosition(middle - 1);
                        int b = getByte();
                        if ((b & 128) == 0) {
                            break;
                        } else {
                            middle--;
                            setPosition(middle);
                        }
                    }

                    uint64_t value = getVLong2();
                    if (value == valueToSearch) {
                        currentValue2 = c2;
                        return;
                    } else if (value < valueToSearch) {
                        s = getCurrentPosition();
                    } else {
                        e = middle;
                    }
                }
                setPosition(s);
            }
        }

        //Not found continue with linear search
        while (more_data() && currentValue2 < c2 && currentValue1 == c1) {
            next_pair();
        }

    } else {
        BOOST_LOG_TRIVIAL(error) << "Not supported";
        throw 10;
    }
}

void SimplifiedGroupPairHandler::mark() {
    PairHandler::mark();
    m_idxNextPointer = idxNextPointer;
    m_startfile2 = startfile2;
    m_startpos2 = startpos2;
    m_endfile2 = endfile2;
    m_endpos2 = endpos2;
    m_firstValue2 = firstValue2;
    mSecondTermIndex = secondTermIndex;
    m_lastSecondTermIndex = lastSecondTermIndex;
    m_lastSecondTermIndexPos = lastSecondTermIndexPos;
}

void SimplifiedGroupPairHandler::reset() {
    idxNextPointer = m_idxNextPointer;
    startpos2 = m_startpos2;
    startfile2 = m_startfile2;
    endpos2 = m_endpos2;
    endfile2 = m_endfile2;
    firstValue2 = m_firstValue2;
    secondTermIndex = mSecondTermIndex;
    lastSecondTermIndex = m_lastSecondTermIndex;
    lastSecondTermIndexPos = m_lastSecondTermIndexPos;
    PairHandler::reset();
}

void SimplifiedGroupPairHandler::setCompressionMode(int v1, int v2) {
    compr1 = v1;
    compr2 = v2;
}

uint64_t SimplifiedGroupPairHandler::estimateNPairs() {
    if (secondTermIndex != NULL && secondTermIndex->sizeIndex() > 0) {
        return (secondTermIndex->sizeIndex() + 1) * ADDITIONAL_SECOND_INDEX_SIZE;
    } else {
        uint64_t diff = endpos2 - startpos2;
        if (diff <= 255) {
            return diff;
        } else {
            return diff / 2;
        }
    }
}

void SimplifiedGroupPairHandler::startAppend() {
    tmpfirstpairs.clear();
    tmpsecondpairs.clear();
    largestElement = 0;
}

void SimplifiedGroupPairHandler::append(long t1, long t2) {
    if (tmpfirstpairs.size() == 0 || tmpfirstpairs.at(tmpfirstpairs.size() - 1).first != t1) {
        tmpfirstpairs.push_back(std::make_pair(t1, tmpsecondpairs.size()));
    }
    tmpsecondpairs.push_back(t2);
    if (t2 >= largestElement) {
        largestElement = t2;
    }
}

uint8_t SimplifiedGroupPairHandler::getNBytes(const int comprType, const long value) const {
    switch (comprType) {
    case COMPR_1:
        return (uint8_t) Utils::numBytes(value);
    case COMPR_2:
        return (uint8_t) Utils::numBytes2(value);
    case NO_COMPR:
        return 8;
    }
    BOOST_LOG_TRIVIAL(error) << "Should not happen";
    throw 10;
}

void SimplifiedGroupPairHandler::stopAppend() {

    createNewFileIfCurrentIsTooLarge();

    uint64_t firstElement = tmpfirstpairs[0].first;
    writeFirstTerm(firstElement);

    short fileCoordinates = getCurrentFile();
    size_t posCoordinates = getCurrentPosition();

    if (tmpfirstpairs.size() > 1) {
        uint64_t diff = tmpfirstpairs[tmpfirstpairs.size() - 1].first - firstElement;
        bytesPerFirstEntry = getNBytes(compr1, diff);
        const uint64_t sizeSecondElements = tmpsecondpairs.size() *
                                            getNBytes(compr2, largestElement);
        bytesPerPointer = (uint8_t) Utils::numBytes2(sizeSecondElements);

        writeByte((uint8_t) (bytesPerFirstEntry << 4 | bytesPerPointer));
        writeVLong2(tmpfirstpairs.size());

        createNewFileIfCurrentIsTooLarge();
        fileCoordinates = getCurrentFile();
        posCoordinates = getCurrentPosition();

        //Write all the other first terms
        vector<std::pair<uint64_t, uint64_t>>::iterator itr = tmpfirstpairs.begin();
        itr++;
        uint64_t i = 1;
        for (; itr != tmpfirstpairs.end(); ++itr) {
            if (i % FIRST_INDEX_SIZE == 0) {
                index->add(itr->first - 1, getCurrentFile(), getCurrentPosition());
            }

            //reserve space for pointer
            const int currentPos = getCurrentPosition();
            reserveBytes(bytesPerPointer);
            assert((currentPos + bytesPerPointer) == getCurrentPosition());

            //write diff first term
            writeFirstTerm(itr->first - firstElement);
            uint8_t bytesToFill = (uint8_t) (currentPos + bytesPerPointer +
                                             bytesPerFirstEntry - getCurrentPosition());
            assert(bytesToFill < bytesPerFirstEntry);
            while (bytesToFill-- > 0) {
                writeByte(0);
            }
            i++;
            createNewFileIfCurrentIsTooLarge();
        }
    } else {
        writeByte(0);
    }


    createNewFileIfCurrentIsTooLarge();
    std::vector<uint64_t>::iterator itr = tmpsecondpairs.begin();
    std::vector<std::pair<uint64_t, uint64_t>>::iterator itrFirst = tmpfirstpairs.begin();

    size_t currentCoordinate = 0;
    uint64_t firstSecondTerm = 0;
    bool addPos = true;

    long bp = getCurrentPosition();
    short bpf = getCurrentFile();
    uint64_t nElements = 0;
    uint64_t nElementsGroup = 0;

    FileIndex *additionalIdx = NULL;
    uint64_t keyAdditionalIdx;

    //Write the second elements
    for (; itr != tmpsecondpairs.end(); ++itr) {
        if (addPos && itrFirst->second == nElements) {
            if (additionalIdx != NULL) {
                index->addAdditionalIndex(keyAdditionalIdx, additionalIdx);
                additionalIdx = NULL;
            }

            keyAdditionalIdx = itrFirst->first;
            itrFirst++;
            if (itrFirst == tmpfirstpairs.end()) {
                addPos = false;
            }

            nElementsGroup = 0;

            if (nElements != 0) {
                //Update the pointer
                short origFile = getCurrentFile();
                size_t origPos = getCurrentPosition();

                long diff = getNBytesFrom(bpf, bp);
                short fileToWrite = fileCoordinates;
                size_t posToWrite = posCoordinates + currentCoordinate * (bytesPerFirstEntry + bytesPerPointer);
                while (posToWrite >= getFileSize(fileToWrite)) {
                    posToWrite -= getFileSize(fileToWrite);
                    fileToWrite++;
                }
                assert(fileToWrite != fileCoordinates || posToWrite >= posCoordinates);
                overwriteVLong2(fileToWrite, posToWrite, diff);
                currentCoordinate++;

                setPosition(origFile, origPos);
            }
            firstSecondTerm = *itr;
            createNewFileIfCurrentIsTooLarge();
            writeSecondTerm(firstSecondTerm);
        } else {
            createNewFileIfCurrentIsTooLarge();
            writeSecondTerm(*itr - firstSecondTerm);
        }

        nElements++;
        nElementsGroup++;

        if (nElementsGroup % ADDITIONAL_SECOND_INDEX_SIZE == 0) {
            if (additionalIdx == NULL) {
                additionalIdx = new FileIndex();
            }
            additionalIdx->add(*itr, getCurrentFile(), getCurrentPosition());
            firstSecondTerm = *itr;
        }
    }

    if (additionalIdx != NULL) {
        index->addAdditionalIndex(keyAdditionalIdx, additionalIdx);
    }
    tmpfirstpairs.clear();
    tmpsecondpairs.clear();
}

void SimplifiedGroupPairHandler::writeFirstTerm(long t) {
    switch (compr1) {
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

void SimplifiedGroupPairHandler::writeSecondTerm(long t) {
    switch (compr2) {
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

long SimplifiedGroupPairHandler::readFirstTerm() {
    switch (compr1) {
    case COMPR_1:
        return getVLong();
    case COMPR_2:
        return getVLong2();
    case NO_COMPR:
        return getLong();
    }
    BOOST_LOG_TRIVIAL(error) << "Not allowed";
    throw 10;
}

long SimplifiedGroupPairHandler::readSecondTerm() {
    switch (compr2) {
    case COMPR_1:
        return getVLong();
    case COMPR_2:
        return getVLong2();
    case NO_COMPR:
        return getLong();
    }
    BOOST_LOG_TRIVIAL(error) << "Not allowed";
    throw 10;
}

void SimplifiedGroupPairHandler::columnNotIn11(SimplifiedGroupPairHandler *p1,
        SimplifiedGroupPairHandler *p2,
        SequenceWriter *output) {

    //boost::chrono::system_clock::time_point start = timens::system_clock::now();

    p1->setPosition(p1->startfile1, p1->startpos1 + p1->bytesPerPointer);
    p2->setPosition(p2->startfile1, p2->startpos1 + p2->bytesPerPointer);
    const size_t blockbyte1 = p1->bytesPerFirstEntry + p1->bytesPerPointer;
    const size_t blockbyte2 = p2->bytesPerFirstEntry + p2->bytesPerPointer;
    const long f1 = p1->firstValue1;
    const long f2 = p2->firstValue1;
    long tv = f1;
    long ov = f2;
    bool isFinished = false;

    //long count = 0;
    //long count2 = 0;

    size_t pos1 = p1->getCurrentPosition();
    size_t pos2 = p2->getCurrentPosition();

    do {
        const bool shiftB = tv >= ov;
        if (tv < ov) {
            output->add(tv);
        }
        if (tv <= ov) {
            if (pos1 < endpos1) {
                //count++;
                p1->setPosition(pos1);
                tv = f1 + p1->readFirstTerm();
                pos1 += blockbyte1;
            } else {
                isFinished = true;
                break;
            }
        }
        if (shiftB) {
            if (pos2 < p2->endpos1) {
                //count2++;
                p2->setPosition(pos2);
                ov = f2 + p2->readFirstTerm();
                pos2 += blockbyte2;
            } else {
                break;
            }
        }
    } while (true);

    if (!isFinished) {
        output->add(tv);
        while (pos1 < endpos1) {
            setPosition(pos1);
            tv = f1 + p1->readFirstTerm();
            output->add(tv);
            pos1 += blockbyte1;
        }
    }

    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
    //                                      - start;
    //cout << "C1=" << count << " C2=" << count2 <<  " " << sec.count() * 1000 << " " << output->size() << endl;
}

void SimplifiedGroupPairHandler::columnNotIn(uint8_t columnId,
        PairHandler *other,
        uint8_t columnOther, SequenceWriter *output) {
    if (other->getType() != COLUMN_LAYOUT) {
        throw 10;
    }
    SimplifiedGroupPairHandler *castedOther =
        (SimplifiedGroupPairHandler*) other;

    //Start the scan. Which fields to read?
    if (columnId == 1) {
        if (columnOther == 1) {

            if (startfile1 == endfile1 && castedOther->startfile1 ==
                    castedOther->startfile1) {
                columnNotIn11(this, castedOther, output);
            } else {
                /*** In both cases read the first fields ***/
                setPosition(startfile1, startpos1);
                castedOther->setPosition(castedOther->startfile1,
                                         castedOther->startpos1);

                //Move to the right location
                if (getCurrentPosition() + bytesPerPointer >=
                        getFileSize(getCurrentFile())) {
                    setPosition(getCurrentFile() + 1,
                                getCurrentPosition() + bytesPerPointer -
                                getFileSize(getCurrentFile()));
                } else {
                    setPosition(getCurrentPosition() + bytesPerPointer);
                }
                if (castedOther->getCurrentPosition() + castedOther->bytesPerPointer >=
                        castedOther->getFileSize(castedOther->getCurrentFile())) {
                    castedOther->setPosition(castedOther->getCurrentFile() + 1,
                                             castedOther->getCurrentPosition() +
                                             castedOther->bytesPerPointer -
                                             castedOther->getFileSize(
                                                 castedOther->getCurrentFile()));
                } else {
                    castedOther->setPosition(castedOther->getCurrentPosition() +
                                             castedOther->bytesPerPointer);
                }

                long tv = firstValue1;
                long ov = castedOther->firstValue1;

                const size_t blockbyte1 = bytesPerFirstEntry + bytesPerPointer;
                const size_t blockbyte2 = castedOther->bytesPerFirstEntry +
                                          castedOther->bytesPerPointer;


                do {
                    const bool shiftA = tv <= ov;
                    const bool shiftB = tv >= ov;
                    if (tv < ov) {
                        output->add(tv);
                    }
                    if (shiftA) {

                        if (getCurrentFile() < endfile1 ||
                                getCurrentPosition() < endpos1) {
                            size_t pos = getCurrentPosition();
                            tv = firstValue1 + readFirstTerm();
                            pos += blockbyte1;
                            if (pos >= getFileSize(getCurrentFile())) {
                                pos -= getFileSize(getCurrentFile());
                                setPosition(getCurrentFile() + 1, pos);
                            } else {
                                setPosition(pos);
                            }
                        } else {
                            break;
                        }
                    }
                    if (shiftB) {
                        if (castedOther->getCurrentFile() < castedOther->endfile1
                                || castedOther->getCurrentPosition() <
                                castedOther->endpos1) {
                            size_t pos = castedOther->getCurrentPosition();
                            ov = castedOther->firstValue1 + castedOther
                                 ->readFirstTerm();
                            pos += blockbyte2;
                            if (pos >= castedOther->getFileSize(
                                        castedOther->getCurrentFile())) {
                                pos -= castedOther->getFileSize(
                                           castedOther->getCurrentFile());
                                castedOther->setPosition(
                                    castedOther->getCurrentFile() + 1, pos);
                            } else {
                                castedOther->setPosition(pos);
                            }
                        } else {
                            ov = LONG_MAX;
                        }
                    }
                } while (true);
            }

        } else {
            /*** First field uses first column, the other the second ***/
            setPosition(startfile1, startpos1);

            //Move to the right location
            if (getCurrentPosition() + bytesPerPointer >=
                    getFileSize(getCurrentFile())) {
                setPosition(getCurrentFile() + 1,
                            getCurrentPosition() + bytesPerPointer -
                            getFileSize(getCurrentFile()));
            } else {
                setPosition(getCurrentPosition() + bytesPerPointer);
            }
            long tv = firstValue1;
            long ov = castedOther->value2();
            long firstTermCastedOther = castedOther->value1();
            const size_t blockbyte1 = bytesPerFirstEntry + bytesPerPointer;

            do {
                const bool shiftA = tv <= ov;
                const bool shiftB = tv >= ov;
                if (tv < ov) {
                    output->add(tv);
                }
                if (shiftA) {
                    if (getCurrentFile() < endfile1 ||
                            getCurrentPosition() < endpos1) {
                        size_t pos = getCurrentPosition();
                        tv = firstValue1 + readFirstTerm();
                        pos += blockbyte1;
                        if (pos >= getFileSize(getCurrentFile())) {
                            pos -= getFileSize(getCurrentFile());
                            setPosition(getCurrentFile() + 1, pos);
                        } else {
                            setPosition(pos);
                        }
                    } else {
                        break;
                    }
                }
                if (shiftB) {
                    if (castedOther->more_data()) {
                        castedOther->next_pair();
                        if (castedOther->value1() != firstTermCastedOther)
                            ov = LONG_MAX;
                        else
                            ov = castedOther->value2();
                    } else {
                        ov = LONG_MAX;
                    }
                }
            } while (true);

        }
    } else {
        if (columnOther == 1) {
            castedOther->setPosition(castedOther->startfile1,
                                     castedOther->startpos1);

            if (castedOther->getCurrentPosition() + castedOther->bytesPerPointer >=
                    castedOther->getFileSize(castedOther->getCurrentFile())) {
                castedOther->setPosition(castedOther->getCurrentFile() + 1,
                                         castedOther->getCurrentPosition() +
                                         castedOther->bytesPerPointer -
                                         castedOther->getFileSize(
                                             castedOther->getCurrentFile()));
            } else {
                castedOther->setPosition(castedOther->getCurrentPosition() +
                                         castedOther->bytesPerPointer);
            }

            long tv = value2();
            long firstTerm = value1();
            long ov = castedOther->firstValue1;

            const size_t blockbyte2 = castedOther->bytesPerFirstEntry +
                                      castedOther->bytesPerPointer;


            do {
                const bool shiftA = tv <= ov;
                const bool shiftB = tv >= ov;
                if (tv < ov) {
                    output->add(tv);
                }
                if (shiftA) {
                    if (more_data()) {
                        next_pair();
                        if (value1() != firstTerm) {
                            break;
                        } else {
                            tv = value2();
                        }

                    } else {
                        break;
                    }
                }
                if (shiftB) {
                    if (castedOther->getCurrentFile() < castedOther->endfile1
                            || castedOther->getCurrentPosition() <
                            castedOther->endpos1) {
                        size_t pos = castedOther->getCurrentPosition();
                        ov = castedOther->firstValue1 + castedOther
                             ->readFirstTerm();
                        pos += blockbyte2;
                        if (pos >= castedOther->getFileSize(
                                    castedOther->getCurrentFile())) {
                            pos -= castedOther->getFileSize(
                                       castedOther->getCurrentFile());
                            castedOther->setPosition(
                                castedOther->getCurrentFile() + 1, pos);
                        } else {
                            castedOther->setPosition(pos);
                        }
                    } else {
                        ov = LONG_MAX;
                    }
                }
            } while (true);

        } else {
            /*** First field uses second column, the other the second ***/
            long tv = value2();
            long ov = castedOther->value2();
            long firstTerm = value1();
            long firstTermCastedOther = castedOther->value1();
            do {
                const bool shiftA = tv <= ov;
                const bool shiftB = tv >= ov;
                if (tv < ov) {
                    output->add(tv);
                }
                if (shiftA) {
                    if (more_data()) {
                        next_pair();
                        if (value1() != firstTerm) {
                            break;
                        } else {
                            tv = value2();
                        }

                    } else {
                        break;
                    }
                }
                if (shiftB) {
                    if (castedOther->more_data()) {
                        castedOther->next_pair();
                        if (castedOther->value1() != firstTermCastedOther)
                            ov = LONG_MAX;
                        else
                            ov = castedOther->value2();
                    } else {
                        ov = LONG_MAX;
                    }
                }
            } while (true);

        }
    }
}

uint64_t SimplifiedGroupPairHandler::getNFirstColumn() {
    size_t sz;
    if (startfile1 != endfile1) {
        sz = getFileSize(startfile1) - startpos1;
        for (short i = startfile1 + 1; i < endfile1; i++) {
            sz += getFileSize(i);
        }
        sz += endpos1;
    } else {
        sz = endpos1 - startpos1;
    }
    const size_t blockbyte1 = bytesPerFirstEntry + bytesPerPointer;
    if (blockbyte1 == 0) {
        assert(sz == 0);
        return 0;
    }

    return sz / blockbyte1 + 1;
}
