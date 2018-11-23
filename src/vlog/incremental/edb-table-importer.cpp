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

size_t EDBimporter::countCardinality(const Literal &query) const {
    size_t card = 0;
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getIterator(query);
    while (iter->hasNext()) {
        iter->next();
        ++card;
    }

    return card;
}

size_t EDBimporter::getCardinality(const Literal &query) {
    LOG(INFOL) << "Need to consider possible Removals";
    PredId_t pred = query.getPredicate().getId();
    if (! layer->hasRemoveLiterals(pred)) {
        LOG(ERRORL) << "Derive cardinality without considering query???";
        return edbTable->getCardinality(query);
    }

    return countCardinality(query);
}
