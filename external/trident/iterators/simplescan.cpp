#include <trident/iterators/simplescanitr.h>
#include <trident/tree/treeitr.h>
#include <trident/kb/querier.h>

#include <tridentcompr/utils/lz4io.h>

#include <iostream>

using namespace std;

void SimpleScanItr::init(Querier *q) {
    this->q = q;
    reader = new LZ4Reader(q->getPathRawData());
}

/*uint64_t SimpleScanItr::estimateCardinality() {
    return q->getInputSize();
}*/

uint64_t SimpleScanItr::getCardinality() {
    return q->getInputSize();
}

bool SimpleScanItr::hasNext() {
    return !reader->isEof();
}

void SimpleScanItr::next() {
    setKey(reader->parseVLong());
    v1 = reader->parseVLong();
    v2 = reader->parseVLong();
}

void SimpleScanItr::clear() {
    delete reader;
}

void SimpleScanItr::mark() {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}

void SimpleScanItr::reset(const char i) {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}

void SimpleScanItr::gotoFirstTerm(long c1) {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}

void SimpleScanItr::gotoSecondTerm(long c2) {
    BOOST_LOG_TRIVIAL(error) << "Not (yet) implemented";
    throw 10;
}
