#include <trident/binarytables/columntable.h>
#include <trident/binarytables/clustertable.h>

void ColumnTable::first() {
    advance();
    currentValue1 = firstValue1 = readFirstTerm();
    countFirstValue1 = getVLong2();

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

    idxNextPointer = 0;
    uint8_t flg = (uint8_t) getByte();
    if (flg) {
        bytesPerNElements = flg;
        flg = (uint8_t) getByte();
        bytesPerFirstEntry = flg >> 4;
        bytesPerPointer = flg & 15;
        nFirstEntries = getVLong2();

        startfile1 = endfile1 = getCurrentFile();
        startpos1 = getCurrentPosition();
        if (startpos1 >= getFileSize(startfile1)) {
            startpos1 -= getFileSize(startfile1);
            startfile1++;
        }
        endpos1 = startpos1 + (nFirstEntries - 1) * (bytesPerFirstEntry + bytesPerPointer + bytesPerNElements);
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
        while (endpos2 >= getFileSize(endfile2)) {
            endpos2 -= getFileSize(endfile2);
            endfile2++;
        }
    } else {
        bytesPerNElements = bytesPerFirstEntry = bytesPerPointer = 0;
        nFirstEntries = 1;

        startfile2 = startfile1 = endfile1 = getCurrentFile();
        startpos2 = startpos1 = endpos1 = getCurrentPosition();
        if (startpos2 == getFileSize(startfile2)) {
            startpos2 = 0;
            startfile2++;
        }
        endfile2 = getLimitFile();
        endpos2 = getLimitPosition();
    }

    //Set the position to the beginning of the first second element
    setPosition(startfile2, startpos2);

    //Read the second entry
    next_pair();
}

long ColumnTable::getCount() {
    if (currentValue1 == firstValue1) {
        return countFirstValue1;
    } else {
        const short f = getCurrentFile();
        const int p = getCurrentPosition();

        short f1 = startfile1;
        int p1 = startpos1 + (idxNextPointer - 1) *
            (bytesPerFirstEntry + bytesPerPointer + bytesPerNElements)
            + bytesPerFirstEntry + bytesPerPointer;

        while (p1 >= getFileSize(f1)) {
            p1 -= getFileSize(f1);
            f1++;
        }
        setPosition(f1, p1);
        long n = getVLong2();
        setPosition(f, p);
        return n;
    }
}

void ColumnTable::next_pair() {
    if (!has_next()) {
        return;
    }

    advance();

    //If pos==index then move to the next first value
    if (idxNextPointer < nFirstEntries - 1 && ((getCurrentFile() == endfile2 &&
                    getCurrentPosition() == endpos2) || isSecondColumnIgnored())) {
        short origFile =
            getCurrentFile();
        size_t origPos = getCurrentPosition();

        //Go back to read the next first term
        short fileNextFirstTerm = startfile1;
        uint64_t posNextFirstTerm = startpos1 + bytesPerPointer +
            (bytesPerPointer + bytesPerFirstEntry
             + bytesPerNElements) * idxNextPointer;
        while (posNextFirstTerm >= getFileSize(fileNextFirstTerm)) {
            posNextFirstTerm -= getFileSize(fileNextFirstTerm);
            fileNextFirstTerm++;
        }
        setPosition(fileNextFirstTerm, posNextFirstTerm);
        currentValue1 = readFirstTerm() + firstValue1;

        idxNextPointer++;

        if (!isSecondColumnIgnored()) {
            if (idxNextPointer < nFirstEntries - 1) {
                //Read the limit for the next group
                short filePointer = fileNextFirstTerm;
                uint64_t posPointer = posNextFirstTerm + bytesPerFirstEntry + bytesPerNElements;
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
    } else if (isSecondColumnIgnored()) {
        //The element before was the last one. Move to the end of the segment
        setPosition(getLimitFile(), getLimitPosition());
    }

    if (isSecondColumnIgnored())
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

void ColumnTable::ignoreSecondColumn() {
    BinaryTable::ignoreSecondColumn();

    //Move the pointer to the current first entry
    short fileNextFirstTerm = startfile1;
    uint64_t posNextFirstTerm = startpos1 + bytesPerPointer +
        (bytesPerPointer +
         bytesPerFirstEntry +
         bytesPerNElements) * idxNextPointer;
    while (posNextFirstTerm >= getFileSize(fileNextFirstTerm)) {
        posNextFirstTerm -= getFileSize(fileNextFirstTerm);
        fileNextFirstTerm++;
    }
    setPosition(fileNextFirstTerm, posNextFirstTerm);

    //Set new limit
    setLimitPosition(endfile1, endpos1);
}

void ColumnTable::setGroup(const short file,
        const size_t pos,
        const size_t blockSize) {

    if (!isSecondColumnIgnored()) {
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
        while (endpos2 >= getFileSize(endfile2)) {
            endpos2 -= getFileSize(endfile2);
            endfile2++;
        }
    } else {
        endfile2 = getLimitFile();
        endpos2 = getLimitPosition();
    }

    //Move to second pair, and read it.
    if (!isSecondColumnIgnored()) {
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
}

void ColumnTable::moveToClosestFirstTerm(long c1) {
    if (c1 < currentValue1)
        return;

    if (c1 == firstValue1) {
        //Set endfile2,endpos2
        if (nFirstEntries == 1) {
            endfile2 = getLimitFile();
            endpos2 = getLimitPosition();
        } else {
            setPosition(startfile1, startpos1);
            endfile2 = endfile1;
            endpos2 = endpos1 + getVLong2();
            while (endpos2 >= getFileSize(endfile2)) {
                endpos2 -= getFileSize(endfile2);
                endfile2++;
            }
        }


        currentValue1 = firstValue1;
        setPosition(endfile1, endpos1);
        advance();
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
    size_t blockSize = bytesPerFirstEntry + bytesPerPointer + bytesPerNElements;
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
    while (has_next() && currentValue1 < c1) {
        next();
    }
}

void ColumnTable::moveToClosestSecondTerm(long c1, long c2) {
    assert(!isSecondColumnIgnored());
    if (c2 <= currentValue2) {
        return;
    }

    if (compr2 == COMPR_1) {
        //Only linear search is possible
        while (has_next() && currentValue2 < c2) {
            next();
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
                size_t offset = ClusterTable::search_value(buffer(), ss,
                        se, valueToSearch);
                if (offset < se) {
                    setPosition(startPos + offset);
                    const long closestValue = getVLong2();
                    currentValue2 = closestValue + firstValue2;
                    if (closestValue == valueToSearch) {
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
                    currentValue2 = value + firstValue2;
                    if (value == valueToSearch) {
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
        while (has_next() && currentValue2 < c2 && currentValue1 == c1) {
            next();
        }

    } else {
        BOOST_LOG_TRIVIAL(error) << "Not supported";
        throw 10;
    }
}

void ColumnTable::mark() {
    BinaryTable::mark();
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

void ColumnTable::reset(const char i) {
    idxNextPointer = m_idxNextPointer;
    startpos2 = m_startpos2;
    startfile2 = m_startfile2;
    endpos2 = m_endpos2;
    endfile2 = m_endfile2;
    firstValue2 = m_firstValue2;
    secondTermIndex = mSecondTermIndex;
    lastSecondTermIndex = m_lastSecondTermIndex;
    lastSecondTermIndexPos = m_lastSecondTermIndexPos;
    BinaryTable::reset(i);
}

void ColumnTable::setCompressionMode(int v1, int v2) {
    compr1 = v1;
    compr2 = v2;
}

long ColumnTable::readFirstTerm() {
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

long ColumnTable::readSecondTerm() {
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

void ColumnTable::columnNotIn11(ColumnTable *p1,
        ColumnTable *p2,
        SequenceWriter *output) {

    //boost::chrono::system_clock::time_point start = timens::system_clock::now();

    p1->setPosition(p1->startfile1, p1->startpos1 + p1->bytesPerPointer);
    p2->setPosition(p2->startfile1, p2->startpos1 + p2->bytesPerPointer);
    const size_t blockbyte1 = p1->bytesPerFirstEntry + p1->bytesPerPointer +
        p1->bytesPerNElements;
    const size_t blockbyte2 = p2->bytesPerFirstEntry + p2->bytesPerPointer +
        p2->bytesPerNElements;
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
}

void ColumnTable::columnNotIn(uint8_t columnId,
        BinaryTable *other,
        uint8_t columnOther, SequenceWriter *output) {
    if (other->getType() != COLUMN_ITR) {
        throw 10;
    }
    ColumnTable *castedOther =
        (ColumnTable*) other;

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

                const size_t blockbyte1 = bytesPerFirstEntry + bytesPerPointer
                    + bytesPerNElements;
                const size_t blockbyte2 = castedOther->bytesPerFirstEntry +
                    castedOther->bytesPerPointer +
                    castedOther->bytesPerNElements;

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
            long ov = castedOther->getValue2();
            long firstTermCastedOther = castedOther->getValue1();
            const size_t blockbyte1 = bytesPerFirstEntry + bytesPerPointer +
                bytesPerNElements;

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
                    if (castedOther->has_next()) {
                        castedOther->next_pair();
                        if (castedOther->getValue1() != firstTermCastedOther)
                            ov = LONG_MAX;
                        else
                            ov = castedOther->getValue2();
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

            long tv = getValue2();
            long firstTerm = getValue1();
            long ov = castedOther->firstValue1;

            const size_t blockbyte2 = castedOther->bytesPerFirstEntry +
                castedOther->bytesPerPointer + castedOther->bytesPerNElements;


            do {
                const bool shiftA = tv <= ov;
                const bool shiftB = tv >= ov;
                if (tv < ov) {
                    output->add(tv);
                }
                if (shiftA) {
                    if (has_next()) {
                        next_pair();
                        if (getValue1() != firstTerm) {
                            break;
                        } else {
                            tv = getValue2();
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
            long tv = getValue2();
            long ov = castedOther->getValue2();
            long firstTerm = getValue1();
            long firstTermCastedOther = castedOther->getValue1();
            do {
                const bool shiftA = tv <= ov;
                const bool shiftB = tv >= ov;
                if (tv < ov) {
                    output->add(tv);
                }
                if (shiftA) {
                    if (has_next()) {
                        next_pair();
                        if (getValue1() != firstTerm) {
                            break;
                        } else {
                            tv = getValue2();
                        }

                    } else {
                        break;
                    }
                }
                if (shiftB) {
                    if (castedOther->has_next()) {
                        castedOther->next();
                        if (castedOther->getValue1() != firstTermCastedOther)
                            ov = LONG_MAX;
                        else
                            ov = castedOther->getValue2();
                    } else {
                        ov = LONG_MAX;
                    }
                }
            } while (true);

        }
    }
}

uint64_t ColumnTable::estNFirsts() {
    return nFirstEntries;
}

uint64_t ColumnTable::getNFirsts() {
    return nFirstEntries;
}

uint64_t ColumnTable::estNSecondsFixedFirst() {
    return getCount();
}

uint64_t ColumnTable::getNSecondsFixedFirst() {
    return getCount();
}
