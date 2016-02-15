#include <trident/iterators/termitr.h>
#include <trident/tree/treeitr.h>

void TermItr::init(TableStorage *tables, uint64_t size) {
    this->tables = tables;
    this->nfiles = tables->getLastCreatedFile() + 1;
    this->currentfile = 0;
    this->currentMark = -1;
    this->size = size;

    this->buffer = tables->getBeginTableCoordinates(currentfile);
    this->endbuffer = tables->getEndTableCoordinates(currentfile);
}

bool TermItr::hasNext() {
    if (buffer == endbuffer) {
        currentfile++;
        currentMark = -1;
        if (currentfile >= nfiles)
            return false;
        if (tables->doesFileHaveCoordinates(currentfile)) {
            this->buffer = tables->getBeginTableCoordinates(currentfile);
            this->endbuffer = tables->getEndTableCoordinates(currentfile);
        } else {
            this->buffer = NULL;
            this->endbuffer = NULL;
            return hasNext();
        }
    }
    return true;
}

void TermItr::next() {
    buffer += 2;
    const long key = Utils::decode_long(buffer);
    setKey(key);
    buffer += 9;
    currentMark++;
}

char TermItr::getCurrentStrat() {
    return *(buffer - 1);
}

void TermItr::clear() {
    tables = NULL;
    buffer = endbuffer = NULL;
}

uint64_t TermItr::getCardinality() {
    return size;
}

uint64_t TermItr::estCardinality() {
    throw 10; //not supported
}

void TermItr::mark() {
    throw 10; //not implemented
}

void TermItr::reset(char i) {
    throw 10; //not implemented
}

void TermItr::gotoKey(long keyToSearch) {
    //Is the file within the file range?
    const long lastKey = Utils::decode_long(endbuffer - 9);
    if (keyToSearch > lastKey) {
        if (currentfile < nfiles - 1) {
            int nextfile = currentfile + 1;
            bool ok = true;
            while (!tables->doesFileHaveCoordinates(nextfile)) {
                if (nextfile < nfiles - 1) {
                    nextfile++;
                } else {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                currentfile = nextfile;
                this->buffer = tables->getBeginTableCoordinates(currentfile);
                this->endbuffer = tables->getEndTableCoordinates(currentfile);
                //read the first key
                currentMark = -1;
                //setKey(Utils::decode_long(buffer, 2));
                gotoKey(keyToSearch);
                return;
            }
        }
        //Move to the very last position
        const size_t elemsAhead = (endbuffer - buffer) / 11;
        buffer = endbuffer - 11;
        currentMark += elemsAhead;
        setKey(lastKey);
        return;
    }

    //Binary search
    const char* start = buffer;
    const char* end = endbuffer;
    while (start < end) {
        const size_t nels = (end - start) / 11;
        const char *middle = start + (nels / 2) * 11;
        const long k = Utils::decode_long(middle + 2);
        if (k < keyToSearch) {
            start = middle + 11;
        } else if (k > keyToSearch) {
            end = middle;
        } else {
            start = middle;
            currentMark += (start - buffer) / 11;
            buffer = start;
            return;
        }
    }
    currentMark += (start - buffer) / 11;
    buffer = end;
}

void TermItr::gotoFirstTerm(long c1) {
    throw 10; //not implemented
}

void TermItr::gotoSecondTerm(long c2) {
    throw 10; //not implemented
}
