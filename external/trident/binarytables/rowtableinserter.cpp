#include <trident/binarytables/rowtableinserter.h>

void RowTableInserter::startAppend() {
    previousValue1 = 0;
    nElements = 0;
}

void RowTableInserter::append(long t1, long t2) {
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

void RowTableInserter::stopAppend() {
}

void RowTableInserter::writeFirstTerm(long t) {
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

void RowTableInserter::writeSecondTerm(long t) {
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

void RowTableInserter::setCompressionMode(int v1, int v2) {
    comprValue1 = v1;
    comprValue2 = v2;
}

void RowTableInserter::setDifferenceMode(int d1) {
    diffValue1 = d1;
}
