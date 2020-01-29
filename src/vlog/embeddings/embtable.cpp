#include <vlog/embeddings/embtable.h>

EmbTable::EmbTable(PredId_t predid,
        EDBLayer *layer,
        std::string folder,
        std::string typeemb) : predid(predid), layer(layer), folder(folder),
    entityOrRel(typeemb == "entity") {
        if (entityOrRel) {
            emb = Embeddings<double>::load(folder + "/E");
        } else {
            emb = Embeddings<double>::load(folder + "/R");
        }
    }

void EmbTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

size_t EmbTable::estimateCardinality(const Literal &query) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

size_t EmbTable::getCardinality(const Literal &query) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

size_t EmbTable::getCardinalityColumn(const Literal &query, uint8_t posColumn) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

bool EmbTable::isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

EDBIterator *EmbTable::getIterator(const Literal &query) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

EDBIterator *EmbTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

bool EmbTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

bool EmbTable::getDictText(const uint64_t id, char *text) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

bool EmbTable::getDictText(const uint64_t id, std::string &text) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

uint64_t EmbTable::getNTerms() {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

void EmbTable::releaseIterator(EDBIterator *itr) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

uint64_t EmbTable::getSize() {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

EmbTable::~EmbTable() {
}
