#include <vlog/text/elastictable.h>

#include <kognac/logs.h>

ElasticTable::ElasticTable() {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void ElasticTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

size_t ElasticTable::getCardinality(const Literal &query) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

size_t ElasticTable::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

bool ElasticTable::isEmpty(const Literal &query,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

EDBIterator *ElasticTable::getIterator(const Literal &query){
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

EDBIterator *ElasticTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

bool ElasticTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

bool ElasticTable::getDictText(const uint64_t id, char *text) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

uint64_t ElasticTable::getNTerms() {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

uint64_t ElasticTable::getSize() {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

ElasticTable::~ElasticTable() {
}
