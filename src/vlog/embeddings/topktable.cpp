#include <vlog/embeddings/topktable.h>
#include <vlog/embeddings/topkiterator.h>

TopKTable::TopKTable(PredId_t predid, EDBLayer *layer,
        std::string topk, std::string typeprediction,
        std::string predentities, std::string predrelations) {
    this->predid = predid;
    this->topk = stoi(topk);
    if (typeprediction == "head") {
        this->typeprediction = 0;
    } else {
        this->typeprediction = 1;
    }
    auto epredid= layer->getPredID(predentities);
    etable = (EmbTable*)layer->getEDBTable(epredid).get();
    offsetEtable = etable->getStartOffset();
    auto rpredid= layer->getPredID(predrelations);
    rtable = (EmbTable*)layer->getEDBTable(rpredid).get();
    offsetRtable = rtable->getStartOffset();
    dim = etable->getEmbeddingDimension();
    nentities = etable->getSize();
    nrels = rtable->getSize();

    tester = std::unique_ptr<TranseTester<double>>(
            new TranseTester<double>(
                etable->getEmbeddings(),
                rtable->getEmbeddings()));
    answer = std::unique_ptr<double[]>(new double[dim]);
    scores.resize(nentities);
}

void TopKTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "TopKTable: (query) Not supported";
    throw 10;
}

size_t TopKTable::estimateCardinality(const Literal &query) {
    return getCardinality(query);
}

bool score_sorter(const std::pair<double, int> &a, const std::pair<double, int> &b) {
    return a.first < b.first;
}

size_t TopKTable::getCardinality(const Literal &query) {
    auto v1 = query.getTermAtPos(0);
    auto v2 = query.getTermAtPos(1);
    auto v3 = query.getTermAtPos(2);
    if (v1.isVariable() && v2.isVariable() && v3.isVariable() &&
            v1.getId() != v2.getId() && v2.getId() != v3.getId()) {
        return nentities * nrels & topk;
    } else if (!v1.isVariable() && !v2.isVariable() && v3.isVariable()) {
        return topk;
    }
    LOG(ERRORL) << "TopKTable: (getCardinality) Not supported";
    throw 10;

}

size_t TopKTable::getCardinalityColumn(const Literal &query, uint8_t posColumn) {
    LOG(ERRORL) << "TopKTable: (cardinality column) Not supported";
    throw 10;
}

bool TopKTable::isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    return false; //I assume that the arguments are always an entity and a
    //relation embedding
}

void TopKTable::getScores(Term_t e, Term_t r) {
    //Get the top-k entities
    auto embent = e - offsetEtable;
    auto embrel = r - offsetRtable;
    if (typeprediction == 0) { //Trying to predict the head
        tester->predictS(answer.get(), embrel, dim, embent, dim);
    } else { //Trying to predict the tail
        tester->predictO(embent, dim, embrel, dim, answer.get());
    }
    //Extract all the top-k answers
    for(size_t i = 0; i < nentities; ++i) {
        auto score = tester->closeness(answer.get(), i, dim);
        scores[i] = std::make_pair(score, i);
    }
    //Sort the scores
    std::sort(scores.begin(), scores.end(), score_sorter);
}

EDBIterator *TopKTable::getIterator(const Literal &query) {
    auto v1 = query.getTermAtPos(0);
    auto v2 = query.getTermAtPos(1);
    auto v3 = query.getTermAtPos(2);
    if (!v1.isVariable() && !v2.isVariable()) {
        getScores(v1.getValue(), v2.getValue());
        return new TopKIterator(predid, topk,
                etable->getEntity(v1.getValue()),
                rtable->getEntity(v2.getValue()), scores, false);
    }
    LOG(ERRORL) << "TopKTable: (getIterator) Not supported";
    throw 10;
}

EDBIterator *TopKTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    auto v1 = query.getTermAtPos(0);
    auto v2 = query.getTermAtPos(1);
    auto v3 = query.getTermAtPos(2);
    if (!v1.isVariable() && !v2.isVariable()) {
        getScores(v1.getValue(), v2.getValue());
        return new TopKIterator(predid, topk,
                etable->getEntity(v1.getValue()),
                rtable->getEntity(v2.getValue()), scores, true);
    }
    LOG(ERRORL) << "TopKTable: (sorted iterator) Not supported";
    throw 10;
}

bool TopKTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    return false; //IDs/Texts are available elswhere
}

bool TopKTable::getDictText(const uint64_t id, char *text) {
    return false; //IDs/Texts are available elswhere
}

bool TopKTable::getDictText(const uint64_t id, std::string &text) {
    return false; //IDs/Texts are available elswhere
}

uint64_t TopKTable::getNTerms() {
    return nentities*2 + nrels;
}

void TopKTable::releaseIterator(EDBIterator *itr) {
    delete itr;
}

uint64_t TopKTable::getSize() {
    uint64_t maxnterms = nentities * nrels * topk;
    return maxnterms;
}

TopKTable::~TopKTable() {
}
