#include <trident/binarytables/clustertableinserter.h>
#include <trident/binarytables/fileindex.h>

void ClusterTableInserter::startAppend() {
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

void ClusterTableInserter::append(long t1, long t2) {
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

void ClusterTableInserter::stopAppend() {
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

int ClusterTableInserter::getRelativeSecondTermPos() {
    if (getCurrentFile() == baseSecondTermFile) {
        return getCurrentPosition() - baseSecondTermPos;
    } else {
        return getCurrentPosition();
    }
}

int ClusterTableInserter::getRelativeSecondTermPos(short file, int absPos) {
    if (file == baseSecondTermFile) {
        return absPos - baseSecondTermPos;
    } else {
        return absPos;
    }
}

void ClusterTableInserter::writeNElementsAt(char b, short file, int pos) {
    // Write the number of elements
    overwriteBAt(b, file, pos);
}

void ClusterTableInserter::insertSecondTerm(bool last) {
    int nbytes = writeSecondTerm(calculateSecondTermToWrite(lastSecondTerm));
    updateSecondTermIndex(lastSecondTerm, nbytes, getCurrentFile(),
                          getCurrentPosition());
}

void ClusterTableInserter::updateSecondTermIndex(long lastTermWritten,
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

long ClusterTableInserter::calculateSecondTermToWrite(long term) {
    // First calculate the term to write
    if (previousSecondTerm == -1) {
        previousSecondTerm = term;
    } else {
        term -= previousSecondTerm;
    }
    return term;
}

int ClusterTableInserter::writeSecondTerm(long termToWrite) {
    if (compr2 == COMPR_1) {
        return writeVLong(termToWrite);
    } else if (compr2 == COMPR_2) {
        return writeVLong2(termToWrite);
    } else {
        return writeLong(termToWrite);
    }
}

void ClusterTableInserter::updateFirstTermIndex(const long t1) {
    // Should a write an entry in the index?
    if (nElementsForIndexing >= FIRST_INDEX_SIZE && previousFirstTerm != -1) {
        nElementsForIndexing = 0;
        index->add(t1, getCurrentFile(), getRelativePosition());
    }
}

void ClusterTableInserter::writeFirstTerm(long termToWrite) {
    if (compr1 == COMPR_1) {
        writeVLong(termToWrite);
    } else if (compr1 == COMPR_2) {
        writeVLong2(termToWrite);
    } else {
        writeLong(termToWrite);
    }
}

long ClusterTableInserter::calculateFirstTermToWrite(long termToWrite) {
    // First determine which term to write
    if (diffMode1 == DIFFERENCE && previousFirstTerm != -1) {
        termToWrite -= previousFirstTerm;
    }
    return termToWrite;
}

void ClusterTableInserter::mode_difference(int modeValue1) {
    diffMode1 = modeValue1;
}

void ClusterTableInserter::mode_compression(int compr1, int compr2) {
    this->compr1 = compr1;
    this->compr2 = compr2;
}
