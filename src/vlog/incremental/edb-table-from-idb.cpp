/**
 * In the overdelete and rederive steps DRed algorithm, we require the values
 * of the previous materialization stages, but they are immutable. So we treat
 * them as if they are EDB tables.
 */

#include <vlog/incremental/edb-table-from-idb.h>

#include <vlog/inmemory/inmemorytable.h>

class EDBonIDBIterator : public EDBIterator {
protected:
    const Literal query;
    const PredId_t predid;
    FCIterator idbItr;
    std::shared_ptr<const FCInternalTable> idbInternalTable;
    FCInternalTableItr *idbInternalItr;
    size_t ticks = 0;

public:
    EDBonIDBIterator(const Literal &query, const std::shared_ptr<SemiNaiver> SN) :
            query(EDBonIDBTable::edb2idb(query)),
            predid(query.getPredicate().getId()),
            idbInternalItr(NULL) {
        idbItr = SN->getTable(this->query, 0, SN->getCurrentIteration());
        if (! idbItr.isEmpty()) {
            idbInternalTable = idbItr.getCurrentTable();
            idbInternalItr = idbInternalTable->getIterator();
        }
    }

    virtual bool hasNext() {
        LOG(TRACEL) << "         hasNext() on " << query.tostring();
        if (idbInternalItr == NULL) {
            return false;
        }
        if (idbInternalItr->hasNext()) {
            return true;
        }

        idbInternalTable->releaseIterator(idbInternalItr);
        idbItr.moveNextCount();
        if (idbItr.isEmpty()) {
            idbInternalItr = NULL;
            return false;
        }

        idbInternalTable = idbItr.getCurrentTable();
        idbInternalItr = idbInternalTable->getIterator();

        return idbInternalItr->hasNext();
    }

    virtual void next() {
        LOG(TRACEL) << "         next() on " << query.tostring();
        if (! idbInternalItr->hasNext()) {
            LOG(ERRORL) << "next() on a table that hasNext() = false";
            throw 10;               // follow convention
        }
        ++ticks;
        return idbInternalItr->next();
    }

    virtual Term_t getElementAt(const uint8_t p) {
        LOG(TRACEL) << "get element[" << (int)p << "] of " << query.tostring() << " = " << idbInternalItr->getCurrentValue(p);
        return idbInternalItr->getCurrentValue(p);
    }

    virtual PredId_t getPredicateID() {
        return predid;
    }

    virtual void moveTo(const uint8_t field, const Term_t t) {
        throw 10;
    }

    virtual void skipDuplicatedFirstColumn() {
        LOG(ERRORL) << "FIXME: implement " << typeid(*this).name() << "::" <<
            __func__;
    }

    virtual void clear() {
        LOG(ERRORL) << "FIXME: implement " << typeid(*this).name() << "::" <<
            __func__;
    }

    virtual const char *getUnderlyingArray(uint8_t column) {
        return NULL;
    }

    /*
    No need to override
    virtual std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray(uint8_t column) {
        return std::make_pair(0, make_pair(0, 0));
    }
    */

    virtual ~EDBonIDBIterator() {
        LOG(DEBUGL) << "EDBonIDBIterator: " << query.tostring() <<
            " num rows queried " << ticks;
    }
};


class EDBonIDBSortedIterator : public EDBIterator {
protected:
    const Literal query;
    const std::vector<uint8_t> &fields;
    EDBLayer *layer;
    const PredId_t predid;
    bool ownsTable;
    EDBTable *inmemoryTable;
    EDBIterator *itr;
    size_t ticks = 0;

public:
    EDBonIDBSortedIterator(const Literal &query,
                           const std::vector<uint8_t> &fields,
                           const std::shared_ptr<SemiNaiver> SN,
                           EDBLayer *layer) :
            query(EDBonIDBTable::edb2idb(query)), fields(fields), layer(layer),
            predid(query.getPredicate().getId()), ownsTable(true) {
        LOG(DEBUGL) << "EDBonIDBSortedIterator, query " <<
            this->query.tostring() << ", fields.size() " << fields.size();
        inmemoryTable = createSortedTable(this->query, fields, SN, layer);
        if (inmemoryTable != NULL) {
            itr = inmemoryTable->getSortedIterator(query, fields);
        }
    }

    EDBonIDBSortedIterator(const Literal &query,
                           const std::vector<uint8_t> &fields,
                           EDBLayer *layer,
                           EDBTable *table) :
            query(query), fields(fields), layer(layer),
            predid(query.getPredicate().getId()), ownsTable(false) {
        LOG(DEBUGL) << "cached EDBonIDBSortedIterator";
        inmemoryTable = table;
        itr = inmemoryTable->getSortedIterator(query, fields);
    }

    static EDBTable *createSortedTable(const Literal &query,
                                       const std::vector<uint8_t> &fields,
                                       const std::shared_ptr<SemiNaiver> SN,
                                       EDBLayer *layer) {
        PredId_t predid = query.getPredicate().getId();
        EDBonIDBIterator between(query, SN);
        // Collect matching data. Will be stored in an InmemoryTable.
        std::vector<std::vector<Term_t>> rows;
        std::vector<Term_t> term(query.getTupleSize());
        // need to store all variables, then afterwards sort by fields
        std::vector<bool> isVariable(query.getTupleSize());
        VTuple tuple = query.getTuple();
        for (size_t i = 0; i < query.getTupleSize(); ++i) {
            VTerm vt = tuple.get(i);
            isVariable[i] = vt.isVariable();
            if (! isVariable[i]) {
                // fill out the constants
                term[i] = vt.getValue();
            }
        }
        while (between.hasNext()) {
            between.next();
            size_t nVars = 0;
            for (size_t i = 0; i < query.getTupleSize(); ++i) {
                if (isVariable[i]) {
                    term[i] = between.getElementAt(nVars);
                    ++nVars;
                }
            }
            rows.push_back(term);
        }

        if (false && rows.size() == 0) {
            return NULL;
        } else {
            return new InmemoryTable(predid, rows, layer);
        }
    }

    virtual bool hasNext() {
        LOG(TRACEL) << "         hasNext() on " << query.tostring();
        if (inmemoryTable == NULL) {
            return false;
        }

        return itr->hasNext();
    }

    virtual void next() {
        LOG(TRACEL) << "         next() on " << query.tostring();
        ticks++;
        return itr->next();
    }

    virtual Term_t getElementAt(const uint8_t p) {
        if (fields.size() == 1) {
            LOG(DEBUGL) << "get element[" << (int)p << "] of " << query.tostring() << " = " << itr->getElementAt(p);
        }
        return itr->getElementAt(p);
    }

    virtual PredId_t getPredicateID() {
        return predid;
    }

    virtual void moveTo(const uint8_t field, const Term_t t) {
        throw 10;
    }

    virtual void skipDuplicatedFirstColumn() {
        LOG(ERRORL) << "FIXME: implement " << typeid(*this).name() << "::" <<
            __func__;
    }

    virtual void clear() {
        LOG(ERRORL) << "FIXME: implement " << typeid(*this).name() << "::" <<
            __func__;
    }

    virtual const char *getUnderlyingArray(uint8_t column) {
        return NULL;
    }

    /*
    No need to override
    virtual std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray(uint8_t column) {
        return std::make_pair(0, make_pair(0, 0));
    }
    */

    virtual ~EDBonIDBSortedIterator() {
        if (inmemoryTable != NULL) {
            inmemoryTable->releaseIterator(itr);
            if (ownsTable) {
                LOG(ERRORL) << "Need to cache inmemoryTable for " << query.tostring(NULL, layer) << " fields size " << fields.size();
                delete inmemoryTable;
            }
        }
        LOG(DEBUGL) << "EDBonIDBSortedIterator: layer=" << layer->getName() <<
            " " << query.tostring() << " num rows queried " << ticks;
    }
};


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

    return true;
}


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
    size_t card = 0;
    // Go through the layer to get a Removals-aware iterator
    EDBIterator *iter = layer->getIterator(query);
    while (iter->hasNext()) {
        iter->next();
        ++card;
    }
    layer->releaseIterator(iter);

    return card;
}

size_t EDBonIDBTable::getCardinality(const Literal &query) {
    LOG(INFOL) << "Need to consider possible Removals";
    PredId_t pred = query.getPredicate().getId();
    if (! layer->hasRemoveLiterals(pred)) {
        uint8_t nVars = query.getNVars();
        LOG(ERRORL) << "Derive cardinality (vars " << (int)nVars << ") without considering query " << query.tostring() << "???";
        if (prevSemiNaiver->getProgram()->getPredicateCard(predid) != countCardinality(query)) {
            LOG(ERRORL) << "getProgram card " << prevSemiNaiver->getProgram()->getPredicateCard(predid) << " counted " << countCardinality(query);
            return countCardinality(query);
        }
        return prevSemiNaiver->getProgram()->getPredicateCard(predid);
    }

    return countCardinality(query);
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
            LOG(DEBUGL) << "create new naturalTable";
            naturalTable = EDBonIDBSortedIterator::createSortedTable(
                                query, fields, prevSemiNaiver, layer);
        } else {
            LOG(DEBUGL) << "Use cached naturalTable";
        }
        return new EDBonIDBSortedIterator(query, fields, layer,
                                          naturalTable);
    }

    LOG(DEBUGL) << "Get SortedIterator for query " <<
        query.tostring(NULL, layer) << " fields " << fields.size();
    LOG(INFOL) << "FIXME: implement cache for sorted query";

    return new EDBonIDBSortedIterator(query, fields, prevSemiNaiver, layer);
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
                os << to_string(v);
            }
        }
        os << std::endl;
    }

    layer->releaseIterator(itr);

    return os;
}
