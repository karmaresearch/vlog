#include <vlog/incremental/edb-table-importer.h>


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

    LOG(ERRORL) << "FIXME: Reconstruct fields from posToFilter and valuesToFilter";
    Predicate pred = query.getPredicate();
    VTuple tuple = query.getTuple();
    uint8_t adornment = pred.calculateAdornment(tuple);
    std::vector<uint8_t> fields;
    for (size_t i = 0; i < tuple.getSize(); ++i) {
        if (! (adornment & (0x1 << i))) {
            fields.push_back(i);
        }
    }
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getSortedIterator(query, fields);
    bool empty = ! iter->hasNext();

    layer->releaseIterator(iter);
    
    return empty;
}

size_t EDBimporter::countCardinality(const Literal &query) {
    size_t card = 0;

    LOG(ERRORL) << "FIXME: For now, consider all fields";
    Predicate pred = query.getPredicate();
    VTuple tuple = query.getTuple();
    uint8_t adornment = pred.calculateAdornment(tuple);
    std::vector<uint8_t> fields;
    for (size_t i = 0; i < tuple.getSize(); ++i) {
        if (! (adornment & (0x1 << i))) {
            fields.push_back(i);
        }
    }
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getSortedIterator(query, fields);
    while (iter->hasNext()) {
        iter->next();
        ++card;
    }
    layer->releaseIterator(iter);

    return card;
}

size_t EDBimporter::getCardinality(const Literal &query) {
    LOG(INFOL) << "Need to consider possible Removals";
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
