/**
 * In the overdelete and rederive steps DRed algorithm, we require the values
 * of the previous materialization stages, but they are immutable. So we treat
 * them as if they are EDB tables.
 */

#include <vlog/incremental/edb-table-from-idb.h>

#include <vlog/inmemory/inmemorytable.h>



bool EDBonIDBTable::isEmpty(const Literal &query,
                            std::vector<uint8_t> *posToFilter,
                            std::vector<Term_t> *valuesToFilter) {
    if (posToFilter != NULL) {
        LOG(ERRORL) << "Not implemented:" << __func__;
        throw 10;
    }
    if (valuesToFilter != NULL) {
        LOG(ERRORL) << "FIXME: Reconstruct fields from posToFilter and valuesToFilter";
        throw 10;
    }

    VTuple tuple = query.getTuple();
    std::vector<uint8_t> fields;
    Predicate pred = query.getPredicate();
    uint8_t adornment = pred.calculateAdornment(tuple);
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


/**
 * Transfer an IDB predicate of the previous SemiNaiver to be
 * a new 'EDB' predicate.
 */

EDBonIDBTable::EDBonIDBTable(PredId_t predid, EDBLayer *layer,
              const std::shared_ptr<SemiNaiver> prevSN) :
        predid(predid), layer(layer), prevSemiNaiver(prevSN),
        idbTable(prevSN->getTable(predid, prevSN->getCurrentIteration())),
        naturalTable(NULL) {
    cardinality = prevSN->getProgram()->getPredicateCard(predid);
    LOG(DEBUGL) << "prevSN iteration " << prevSN->getCurrentIteration() <<
        " card. " << cardinality << " size " << idbTable->getNAllRows();
}

EDBonIDBTable::~EDBonIDBTable() {
    delete naturalTable;
}

bool EDBonIDBTable::isNatural(const Literal &query, const std::vector<uint8_t> fields) {
    // Considered 'natural' if all query arguments are variables, and all
    // variables occur in order
    if (query.getTupleSize() != fields.size()) {
        return false;
    }

    for (::size_t i = 0; i < fields.size(); ++i) {
        if (fields[i] != static_cast<uint8_t>(fields[i])) {
            return false;
        }
    }

    if (true) {
        LOG(INFOL) << "For now, disable isNatural query attribute, query " << query.tostring() << " fields " << fields2str(fields);
        return false;
    }

    return true;
}

bool EDBonIDBTable::isNatural(const Literal &query) {
    // Considered as an upper bound for 'natural' if all query arguments are
    // variables
    const VTuple &tuple = query.getTuple();
    for (int i = 0; i < tuple.getSize(); ++i) {
        if (! tuple.get(i).isVariable()) {
            return false;
        }
    }

    if (true) {
        LOG(INFOL) << "For now, disable isNatural query attribute, query " << query.tostring() << " fields " << "<undefined>";
        return false;
    }

    return true;
}

InmemoryTable *EDBonIDBTable::createSortedTable(
                                 const Literal &query,
                                 const std::vector<uint8_t> &fields,
                                 const std::shared_ptr<SemiNaiver> SN,
                                 EDBLayer *layer) const {
    LOG(INFOL) << "Create sorted InmemoryTable, query " << query.tostring() << " fields " << fields2str(fields);
    HiResTimer t_sorter("Sorter " + query.tostring() + fields2str(fields));
    t_sorter.start();

    PredId_t predid = query.getPredicate().getId();

    EDBonIDBIterator between(query, SN);
    InmemoryTable *t = new InmemoryTable(predid, query, &between, layer);

    t_sorter.stop();
    LOG(INFOL) << t_sorter.tostring();

    return t;
}

void EDBonIDBTable::query(QSQQuery *query, TupleTable *outputTable,
                          std::vector<uint8_t> *posToFilter,
                          std::vector<Term_t> *valuesToFilter) {
    LOG(INFOL) << "Cloned this code from InmemoryTable";

    Term_t row[128];
    const Literal *lit = query->getLiteral();
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    size_t sz = lit->getTupleSize();
    EDBIterator *iter = layer->getIterator(*lit);
    if (posToFilter == NULL || posToFilter->size() == 0) {
        while (iter->hasNext()) {
            iter->next();
            for (uint8_t i = 0; i < npos; ++i) {
                row[i] = iter->getElementAt(pos[i]);
            }
            outputTable->addRow(row);
        }
        layer->releaseIterator(iter);
        return;
    }

    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

size_t EDBonIDBTable::countCardinality(const Literal &query) {
    HiResTimer t_count_card("Count Card query " + query.tostring());
    t_count_card.start();
    size_t card = 0;
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getIterator(query);
    while (iter->hasNext()) {
        iter->next();
        ++card;
    }
    layer->releaseIterator(iter);
    t_count_card.stop();
    LOG(INFOL) << t_count_card.tostring() << ", result = " << card;

    return card;
}

size_t EDBonIDBTable::getCardinality(const Literal &query) {
    PredId_t pred = query.getPredicate().getId();
    if (! layer->hasRemoveLiterals(pred)) {
        // size_t v = prevSemiNaiver->estimateCardinality(query, 0, prevSemiNaiver->getCurrentIteration());
        // We need the precise number here. --Ceriel
        size_t v = countCardinality(query);
        return v;
    }

    size_t card;

    LOG(INFOL) << "Need to consider possible Removals";
    if (isNatural(query)) {
        if (naturalTable == NULL) {
            std::vector<uint8_t> fields;
            for (int i = 0; i < query.getTupleSize(); ++i) {
                fields.push_back(i);
            }
            LOG(INFOL) << __func__ << ":create new naturalTable";
            naturalTable = createSortedTable(
                                query, fields, prevSemiNaiver, layer);
        } else {
            LOG(INFOL) << __func__ << "Use cached naturalTable";
        }

        HiResTimer t_get_card("Get Card query " + query.tostring());
        t_get_card.start();
        card = naturalTable->getCardinality(query);
        t_get_card.stop();
        LOG(INFOL) << t_get_card.tostring();

    } else {
        card = countCardinality(query);
    }

    return card;
}

EDBIterator *EDBonIDBTable::getIterator(const Literal &q) {
    LOG(DEBUGL) << "Get iterator for query " << q.tostring(NULL, layer);
    return new EDBonIDBIterator(q, prevSemiNaiver);
}

EDBIterator *EDBonIDBTable::getSortedIterator(const Literal &query,
                                       const std::vector<uint8_t> &fields) {
    if (isNatural(query, fields)) {
        LOG(DEBUGL) << "'Natural' sorted query " << query.tostring(NULL, layer);
        if (naturalTable == NULL) {
            LOG(DEBUGL) << __func__ << ": create new naturalTable";
            naturalTable = createSortedTable(
                                query, fields, prevSemiNaiver, layer);
        } else {
            LOG(INFOL) << __func__ << ": Use cached naturalTable";
        }
        return new EDBonIDBSortedIterator(query, fields, *this, layer,
                                          naturalTable);
    }

    LOG(DEBUGL) << "Get SortedIterator for query " <<
        query.tostring(NULL, layer) << " fields " << fields2str(fields);
    LOG(INFOL) << "FIXME: implement cache for sorted query";

    return new EDBonIDBSortedIterator(query, fields, prevSemiNaiver, *this,
                                      layer);
}

void EDBonIDBTable::releaseIterator(EDBIterator *itr) {

    delete itr;
}

std::ostream &EDBonIDBTable::dump(std::ostream &os) {
    std::string name = layer->getPredName(predid);
    os << (void *)this << " Table/EDBonIDB " << name << std::endl;
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
