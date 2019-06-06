#ifndef INCREMENTAL__EDB_TABLE_FROM_IDB_H__
#define INCREMENTAL__EDB_TABLE_FROM_IDB_H__

/**
 * In the overdelete and rederive steps DRed algorithm, we require the values
 * of the previous materialization stages, but they are immutable. So we treat
 * them as if they are EDB tables.
 */

#include <vlog/fcinttable.h>
#include <vlog/seminaiver.h>

#include <vlog/inmemory/inmemorytable.h>

#include <vlog/hi-res-timer.h>


/**
 * Transfer an IDB predicate of the previous SemiNaiver to be
 * a new 'EDB' predicate.
 */
class EDBonIDBTable : public EDBTable {
private:
    PredId_t    predid;
    const std::shared_ptr<SemiNaiver> prevSemiNaiver;
    const FCTable *idbTable;
    InmemoryTable *naturalTable;
    size_t cardinality;
    EDBLayer *layer;

    // true if all query tuple fields are variables and they are in natural
    // order in fields
    static bool isNatural(const Literal &query,
                          const std::vector<uint8_t> fields);
    // true if all query tuple fields are variables
    static bool isNatural(const Literal &query);

    // should be const
    size_t countCardinality(const Literal &query);

public:
    EDBonIDBTable(PredId_t predid, EDBLayer *layer,
                  const std::shared_ptr<SemiNaiver> prevSN);

    virtual ~EDBonIDBTable();

    //execute the query on the knowledge base
    virtual void query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter);

    size_t estimateCardinality(const Literal &query) {
        return getCardinality(query);
    }

    size_t getCardinality(const Literal &query);

    size_t getCardinalityColumn(const Literal &query, uint8_t posColumn) {
        LOG(ERRORL) << "FIXME: implement " << __func__;
        throw 10;
    }

    bool isEmpty(const Literal &query,
                         std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter);

    EDBIterator *getIterator(const Literal &q);

    EDBIterator *getSortedIterator(const Literal &query,
                                           const std::vector<uint8_t> &fields);

    void releaseIterator(EDBIterator *itr);

    bool getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) {
        return false;
    }

    bool getDictText(const uint64_t id, char *text) {
        return false;
    }

    bool getDictText(const uint64_t id, std::string &text) {
        return false;
    }

    uint64_t getNTerms() {
        return 0;
    }

    uint8_t getArity() const {
        return idbTable->getSizeRow();
    }

    uint64_t getSize() {
        return idbTable->getNAllRows();
    }

    PredId_t getPredicateID() const {
        return predid;
    }

    InmemoryTable *createSortedTable(const Literal &query,
                                     const std::vector<uint8_t> &fields,
                                     const std::shared_ptr<SemiNaiver> SN,
                                     EDBLayer *layer) const;

    static Literal edb2idb(const Literal &query) {
        Predicate edbPred(query.getPredicate());
        Predicate idbPred(edbPred.getId(), edbPred.getAdornment(),
                          (uint8_t)(edbPred.getType() | IDB),
                          edbPred.getCardinality());
        return Literal(idbPred, query.getTuple());
    }

    std::ostream &dump(std::ostream &os);
};

/*
std::ostream &operator<<(ostream &os, const EDBonIDBTable &table) {
    return os << table.dump();
}
*/

class EDBonIDBIterator : public EDBIterator {
protected:
    const Literal query;
    const PredId_t predid;
    FCIterator idbItr;
    std::shared_ptr<const FCInternalTable> idbInternalTable;
    FCInternalTableItr *idbInternalItr;
    size_t ticks = 0;
    Term_t value[256];
    int offsets[256];

public:
    EDBonIDBIterator(const Literal &query, const std::shared_ptr<SemiNaiver> SN) :
            query(EDBonIDBTable::edb2idb(query)),
            predid(query.getPredicate().getId()),
            idbInternalItr(NULL) {
        LOG(DEBUGL) << "EDBonIDBIterator on " << this->query.tostring();
        idbItr = SN->getTable(this->query, 0, SN->getCurrentIteration());
        if (! idbItr.isEmpty()) {
            idbInternalTable = idbItr.getCurrentTable();
            idbInternalItr = idbInternalTable->getIterator();
        }
        int varNo = 0;
        for (int i = 0; i < this->query.getTupleSize(); i++) {
            if (! this->query.getTermAtPos(i).isVariable()) {
                offsets[i] = -1;
                value[i] = this->query.getTermAtPos(i).getValue();
            } else {
                offsets[i] = varNo;
                varNo++;
            }
        }
        /*
        for (int i = 0; i < this->query.getTupleSize(); i++) {
            LOG(DEBUGL) << "i = " << i << ", offsets[i] = " << offsets[i];
        }
        */
    }

    bool hasNext() {
        // LOG(TRACEL) << "         hasNext() on " << query.tostring();
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

    void next() {
        // LOG(TRACEL) << "         next() on " << query.tostring();
        if (! idbInternalItr->hasNext()) {
            LOG(ERRORL) << "next() on a table that hasNext() = false";
            throw 10;               // follow convention
        }
        ++ticks;
        return idbInternalItr->next();
    }

    Term_t getElementAt(const uint8_t p) {
        Term_t v;
        if (offsets[p] == -1) {
            v = value[p];
        } else {
            v = idbInternalItr->getCurrentValue(offsets[p]);
        }
        //LOG(TRACEL) << "get element[" << (int)p << "] of " << query.tostring() << " = " << v;
        return v;
    }

    PredId_t getPredicateID() {
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
        if (idbInternalItr != NULL) {
            idbInternalTable->releaseIterator(idbInternalItr);
            idbInternalItr = NULL;
        }
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

        if (idbInternalItr != NULL) {
            idbInternalTable->releaseIterator(idbInternalItr);
        }
        LOG(DEBUGL) << "EDBonIDBIterator: " << query.tostring() <<
            " num rows queried " << ticks;
    }
};


class EDBonIDBSortedIterator : public EDBIterator {
protected:
    const Literal query;
    const std::vector<uint8_t> &fields;
    const EDBonIDBTable &owner_table;
    EDBLayer *layer;
    const PredId_t predid;
    bool ownsTable;
    InmemoryTable *inmemoryTable;
    EDBIterator *itr;
    size_t ticks = 0;

public:
    EDBonIDBSortedIterator(const Literal &query,
                           const std::vector<uint8_t> &fields,
                           const std::shared_ptr<SemiNaiver> SN,
                           const EDBonIDBTable &owner_table,
                           EDBLayer *layer) :
            query(EDBonIDBTable::edb2idb(query)), fields(fields),
            owner_table(owner_table), layer(layer),
            predid(query.getPredicate().getId()),
            ownsTable(true) {
        LOG(DEBUGL) << "EDBonIDBSortedIterator, query " <<
            this->query.tostring() << " fields " << fields2str(fields);
        inmemoryTable = owner_table.createSortedTable(this->query, fields, SN,
                                                      layer);
        LOG(ERRORL) << "{ " << (uintptr_t)(void *)inmemoryTable;
        if (inmemoryTable != NULL) {
            itr = inmemoryTable->getSortedIterator(query, fields);
        }
    }

    EDBonIDBSortedIterator(const Literal &query,
                           const std::vector<uint8_t> &fields,
                           const EDBonIDBTable &owner_table,
                           EDBLayer *layer,
                           InmemoryTable *table) :
            query(query), fields(fields),
            owner_table(owner_table), layer(layer),
            predid(query.getPredicate().getId()), ownsTable(false) {
        LOG(DEBUGL) << "cached EDBonIDBSortedIterator";
        inmemoryTable = table;
        itr = inmemoryTable->getSortedIterator(query, fields);
    }

    bool hasNext() {
        // LOG(TRACEL) << "         hasNext() on " << query.tostring();
        if (inmemoryTable == NULL) {
            return false;
        }

        return itr->hasNext();
    }

    void next() {
        // LOG(TRACEL) << "         next() on " << query.tostring();
        ticks++;
        return itr->next();
    }

    Term_t getElementAt(const uint8_t p) {
        if (fields.size() == 1) {
            // LOG(TRACEL) << "get element[" << (int)p << "] of " << query.tostring() << " = " << itr->getElementAt(p);
        }
        return itr->getElementAt(p);
    }

    PredId_t getPredicateID() {
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
                LOG(ERRORL) << "Need to cache inmemoryTable for " << query.tostring(NULL, layer) << " fields " << fields2str(fields);
                LOG(ERRORL) << "} " << (uintptr_t)(void *)inmemoryTable;
                delete inmemoryTable;
            }
        }
        LOG(DEBUGL) << "EDBonIDBSortedIterator: layer=" << layer->getName() <<
            " " << query.tostring() << " num rows queried " << ticks;
    }
};

#endif  // INCREMENTAL__EDB_TABLE_FROM_IDB_H__
