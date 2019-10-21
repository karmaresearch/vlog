#include <vlog/incremental/edb-table-importer.h>

#include <vlog/hi-res-timer.h>


EDBimporter::EDBimporter(PredId_t predid, EDBLayer *layer,
                         const std::shared_ptr<SemiNaiver> prevSN) :
        predid(predid), layer(layer), prevSemiNaiver(prevSN),
        edbTable(prevSN->getEDBLayer().getEDBTable(predid)) {
    LOG(DEBUGL) << "EDBImporter constructor";
}

EDBimporter::~EDBimporter() {
    LOG(DEBUGL) << "EDBImporter destructor";
}

bool EDBimporter::isEmpty(const Literal &query,
                          std::vector<uint8_t> *posToFilter,
                          std::vector<Term_t> *valuesToFilter) {
    // fast path
    if (edbTable->isEmpty(query, posToFilter, valuesToFilter)) {
        return true;
    }

    if (posToFilter != NULL) {
        LOG(ERRORL) << "Not implemented:" << __func__;
        throw 10;
    }
    if (valuesToFilter != NULL) {
        LOG(ERRORL) << "FIXME: Reconstruct fields from posToFilter and valuesToFilter";
        throw 10;
    }

    Predicate pred = query.getPredicate();
    VTuple tuple = query.getTuple();
    uint8_t adornment = pred.calculateAdornment(tuple);
    std::vector<uint8_t> fields;
    uint8_t it = 0;
    for (size_t i = 0; i < tuple.getSize(); ++i) {
        if (! (adornment & (0x1 << i))) {
            fields.push_back(it);
            ++it;
        }
    }
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getSortedIterator(query, fields);
    bool empty = ! iter->hasNext();

    layer->releaseIterator(iter);
    
    return empty;
}

size_t EDBimporter::countCardinality(const Literal &query) {
    HiResTimer t_card("EDBImporter count card " + query.tostring());
    t_card.start();
    size_t card = 0;

    Predicate pred = query.getPredicate();
    VTuple tuple = query.getTuple();
    uint8_t adornment = pred.calculateAdornment(tuple);
    std::vector<uint8_t> fields;
    uint8_t it = 0;
    for (size_t i = 0; i < tuple.getSize(); ++i) {
        if (! (adornment & (0x1 << i))) {
            fields.push_back(it);
            ++it;
        }
    }
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getSortedIterator(query, fields);
    while (iter->hasNext()) {
        iter->next();
        ++card;
    }
    layer->releaseIterator(iter);
    t_card.stop();
    LOG(INFOL) << t_card.tostring();

    return card;
}

size_t EDBimporter::getCardinality(const Literal &query) {
    LOG(DEBUGL) << "Need to consider possible Removals";
    PredId_t pred = query.getPredicate().getId();
    if (! layer->hasRemoveLiterals(pred)) {
        uint8_t nVars = query.getNVars();
        LOG(ERRORL) << "Derive cardinality (vars " << (int)nVars << ") without considering query " << query.tostring() << "???";
        if (prevSemiNaiver->getProgram()->getPredicateCard(predid) != countCardinality(query)) {
            LOG(ERRORL) << "getProgram card " << prevSemiNaiver->getProgram()->getPredicateCard(predid) << " counted " << countCardinality(query);
            return countCardinality(query);
        }
        return edbTable->getCardinality(query);
    }

    return countCardinality(query);
}

std::ostream &EDBimporter::dump(std::ostream &os) {
    std::string name = layer->getPredName(predid);
    os << (void *)this << " Table/EDBimporter " << name << std::endl;
    const uint8_t sizeRow = getArity();
    Predicate pred(predid, 0, EDB, sizeRow);
    VTuple t = VTuple(sizeRow);
    for (uint8_t i = 0; i < t.getSize(); ++i) {
        t.set(VTerm(i + 1, 0), i);
    }
    Literal lit(pred, t);
    os << "Literal@" << int(sizeRow) << " " << lit.tostring() << std::endl;
    EDBIterator *itr = layer->getIterator(lit);
    while (itr->hasNext()) {
        itr->next();
        for (uint8_t m = 0; m < sizeRow; ++m) {
            os << "\t";
            uint64_t v = itr->getElementAt(m);
            std::string buffer;
            if (getDictText(v, buffer)) {
                os << buffer;
            } else {
                os << std::to_string(v);
            }
        }
        os << std::endl;
    }

    layer->releaseIterator(itr);

    return os;
}

