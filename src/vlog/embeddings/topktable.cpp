#include <vlog/embeddings/topktable.h>

TopKTable::TopKTable(EDBLayer *layer,
        std::string topk) {
    this->topk = stoi(topk);
}

void TopKTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

size_t TopKTable::estimateCardinality(const Literal &query) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

size_t TopKTable::getCardinality(const Literal &query) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

size_t TopKTable::getCardinalityColumn(const Literal &query, uint8_t posColumn) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

bool TopKTable::isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

EDBIterator *TopKTable::getIterator(const Literal &query) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

EDBIterator *TopKTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

bool TopKTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

bool TopKTable::getDictText(const uint64_t id, char *text) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

bool TopKTable::getDictText(const uint64_t id, std::string &text) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

uint64_t TopKTable::getNTerms() {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

void TopKTable::releaseIterator(EDBIterator *itr) {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

uint64_t TopKTable::getSize() {
    LOG(ERRORL) << "TopKTable: Not supported";
    throw 10;
}

TopKTable::~TopKTable() {
}
